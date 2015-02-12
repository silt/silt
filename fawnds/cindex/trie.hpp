#pragma once

#include "common.hpp"
#include "bit_access.hpp"
#include "huffman.hpp"
#include "exp_golomb.hpp"
#include "sign_interleave.hpp"
#include <iostream>

//#define BENCHMARK
//#define BENCHMARK_FINEGRAIN
#ifdef BENCHMARK
#include "stopwatch.hpp"
#endif

namespace cindex
{
	struct trie_stats
	{
		static uint64_t time_encode_total;
		static uint64_t time_encode_golomb;
		static uint64_t time_encode_huffman;
		static uint64_t time_decode_total;
		static uint64_t time_decode_golomb;
		static uint64_t time_decode_huffman;
		static void print();
	};

	template<bool WeakOrdering = false, unsigned int HuffmanCodingLimit = 16, typename RefType = uint8_t>
	class trie
	{
	public:
		trie()
		{
			// prepare huffman coding for n <= HuffmanCodingLimit
			for (unsigned int n = 2; n <= HuffmanCodingLimit; n++)
			{
				if (!WeakOrdering)
				{
					huffman_tree_generator<uint64_t> gen(n + 1);

					uint64_t v = 1;
					gen[0] = v;
					for (unsigned int k = 1; k <= n; k++)
						gen[k] = v = v * (n - k + 1) / k;

					huffman_tree<RefType> t(n + 1);
					gen.generate(t);

					huff_[n - 2] = new huffman<RefType>(t);
				}
				else
				{
					huffman_tree_generator<uint64_t> gen(n);

					uint64_t v = 1;
					gen[0] = v * 2;
					for (unsigned int k = 1; k <= n - 1; k++)
						gen[k] = v = v * (n - k + 1) / k;

					huffman_tree<RefType> t(n);
					gen.generate(t);

					huff_[n - 2] = new huffman<RefType>(t);
				}
			}
		}

		template<typename DistType>
		void
		recreate_huffman_from_dist(DistType& dist)
		{
			assert(!WeakOrdering);

			for (unsigned int n = 2; n <= HuffmanCodingLimit; n++)
				delete huff_[n - 2];

			for (unsigned int n = 2; n <= HuffmanCodingLimit; n++)
			{
				huffman_tree_generator<uint64_t> gen(n + 1);

				for (unsigned int k = 0; k <= n; k++)
					gen[k] = dist[n][k];

				huffman_tree<RefType> t(n + 1);
				gen.generate(t);

				huff_[n - 2] = new huffman<RefType>(t);
			}
		}

		virtual ~trie()
		{
			for (unsigned int n = 2; n <= HuffmanCodingLimit; n++)
			{
				delete huff_[n - 2];
				huff_[n - 2] = NULL;
			}
		}

		template<typename Buffer, typename KeyArrayType>
		void encode(Buffer& out_buf, const KeyArrayType& arr, std::size_t key_len, std::size_t off, std::size_t n, std::size_t dest_base = 0, std::size_t dest_keys_per_block = 1, std::size_t skip_bits = 0) const
		{
#ifdef BENCHMARK
			scoped_stopwatch sw(trie_stats::time_encode_total);
#endif
			encode_rec(out_buf, arr, key_len, off, n, dest_base, dest_keys_per_block, skip_bits);
		}

		template<typename Buffer>
		std::size_t locate(const Buffer& in_buf, std::size_t& in_out_buf_iter, const uint8_t* key, std::size_t key_len, std::size_t off, std::size_t n, std::size_t dest_base = 0, std::size_t dest_keys_per_block = 1, std::size_t skip_bits = 0) const
		{
#ifdef BENCHMARK
			scoped_stopwatch sw(trie_stats::time_decode_total);
#endif
			return locate_rec(in_buf, in_out_buf_iter, key, key_len, off, n, dest_base, dest_keys_per_block, skip_bits);
		}

	protected:
		template<typename Buffer, typename KeyArrayType>
		void
		encode_rec(Buffer& out_buf, const KeyArrayType& arr, std::size_t key_len, std::size_t off, std::size_t n, std::size_t dest_base, std::size_t dest_keys_per_block, std::size_t depth) const
		{
			//std::cout << "encode: " << off << " " << n << " " << depth << std::endl;

			// do not encode 0- or 1-sized trees
			/*
			if (n == 1 || depth >= 4)
			{
				for (std::size_t k = depth; k < 4; k++)
					out_buf.push_back(0);
				return;
			}
			*/
			if (n <= 1)
				return;

			// k-perfect hashing
			if (n <= dest_keys_per_block && (dest_base + off) / dest_keys_per_block == (dest_base + off + n - 1) / dest_keys_per_block)
				return;

			assert(depth < key_len * 8);    // duplicate key?

			// find the number of keys on the left tree
			std::size_t left = 0;
			for (; left < n; left++)
			{
				if (bit_access::get(arr[off + left], depth))   // assume sorted keys
					break;
			}

			// replace (n, 0) split with (0, n) split if weak ordering is used
			if (WeakOrdering && left == n)
				left = 0;

			// encode the left tree size
			if (n <= HuffmanCodingLimit)
			{
#ifdef BENCHMARK_FINEGRAIN
				scoped_stopwatch sw(trie_stats::time_encode_huffman);
#endif
				huff_[n - 2]->encode(out_buf, left);
			}
			else
			{
#ifdef BENCHMARK_FINEGRAIN
				scoped_stopwatch sw(trie_stats::time_encode_golomb);
#endif
				exp_golomb<>::encode<std::size_t>(out_buf, sign_interleave::encode<std::size_t>(left - n / 2));
			}

			encode_rec(out_buf, arr, key_len, off, left, dest_base, dest_keys_per_block, depth + 1);
			encode_rec(out_buf, arr, key_len, off + left, n - left, dest_base, dest_keys_per_block, depth + 1);
		}

		template<typename Buffer>
		std::size_t
		locate_rec(const Buffer& in_buf, std::size_t& in_out_buf_iter, const uint8_t* key, std::size_t key_len, std::size_t off, std::size_t n, std::size_t dest_base, std::size_t dest_keys_per_block, std::size_t depth) const
		{
			//std::cout << "locate: " << off << " " << n << " " << depth << std::endl;

			// do not encode 0- or 1-sized trees
			if (n <= 1)
				return 0;

			// k-perfect hashing
			if (n <= dest_keys_per_block && (dest_base + off) / dest_keys_per_block == (dest_base + off + n - 1) / dest_keys_per_block)
				return 0;

			assert(depth < key_len * 8);    // invalid code?

			// decode the left tree size
			std::size_t left;
			if (n <= HuffmanCodingLimit)
			{
#ifdef BENCHMARK_FINEGRAIN
				scoped_stopwatch sw(trie_stats::time_decode_huffman);
#endif
				left = huff_[n - 2]->decode(in_buf, in_out_buf_iter);
			}
			else
			{
#ifdef BENCHMARK_FINEGRAIN
				scoped_stopwatch sw(trie_stats::time_decode_golomb);
#endif
				left = sign_interleave::decode<std::size_t>(exp_golomb<>::decode<std::size_t>(in_buf, in_out_buf_iter)) + n / 2;
			}
			assert(left <= n);

			// find the number of keys on the left to the key (considering weak ordering)
			if (!bit_access::get(key, depth) && (!WeakOrdering || (WeakOrdering && left != 0)))
			{
				return locate_rec(in_buf, in_out_buf_iter, key, key_len, off, left, dest_base, dest_keys_per_block, depth + 1);
			}
			else
			{
				skip_rec(in_buf, in_out_buf_iter, key, key_len, off, left, dest_base, dest_keys_per_block, depth + 1);
				return left + locate_rec(in_buf, in_out_buf_iter, key, key_len, off + left, n - left, dest_base, dest_keys_per_block, depth + 1);
			}
		}

		template<typename Buffer>
		void
		skip_rec(const Buffer& in_buf, std::size_t& in_out_buf_iter, const uint8_t* key, std::size_t key_len, std::size_t off, std::size_t n, std::size_t dest_base, std::size_t dest_keys_per_block, std::size_t depth) const
		{
			//std::cout << "skip: " << off << " " << n << " " << depth << std::endl;

			// do not encode 0- or 1-sized trees
			if (n <= 1)
				return;

			// k-perfect hashing
			if (n <= dest_keys_per_block && (dest_base + off) / dest_keys_per_block == (dest_base + off + n - 1) / dest_keys_per_block)
				return;

			assert(depth < key_len * 8);    // invalid code?

			// decode the left tree size
			std::size_t left;
			if (n <= HuffmanCodingLimit)
			{
#ifdef BENCHMARK_FINEGRAIN
				scoped_stopwatch sw(trie_stats::time_decode_huffman);
#endif
				left = huff_[n - 2]->decode(in_buf, in_out_buf_iter);
			}
			else
			{
#ifdef BENCHMARK_FINEGRAIN
				scoped_stopwatch sw(trie_stats::time_decode_golomb);
#endif
				left = sign_interleave::decode<std::size_t>(exp_golomb<>::decode<std::size_t>(in_buf, in_out_buf_iter)) + n / 2;
			}
			assert(left <= n);

			skip_rec(in_buf, in_out_buf_iter, key, key_len, off, left, dest_base, dest_keys_per_block, depth + 1);
			skip_rec(in_buf, in_out_buf_iter, key, key_len, off + left, n - left, dest_base, dest_keys_per_block, depth + 1);
		}

	protected:
		huffman<RefType>* huff_[HuffmanCodingLimit - 1];

	};
}

