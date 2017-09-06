#pragma once

#include "common.hpp"
#include "bit_access.hpp"
#include <map>
#include <cstring>
//#include <iostream>

namespace cindex
{
	template<typename RefType = uint8_t>
	class huffman_tree
	{
	public:
		typedef RefType ref_type;

	public:
		huffman_tree(std::size_t num_symbols)
			: num_symbols_(num_symbols)
		{
			assert(guarded_cast<std::size_t>(guarded_cast<ref_type>(num_symbols_ * 2 - 1))
					== num_symbols_ * 2 - 1);
			nodes_ = new ref_type[(num_symbols_ - 1) * 2];
		}

		huffman_tree(const huffman_tree<ref_type>& t)
			: num_symbols_(t.num_symbols_)
		{
			nodes_ = new ref_type[(num_symbols_ - 1) * 2];
			memcpy(nodes_, t.nodes_, sizeof(ref_type) * (num_symbols_ - 1) * 2);
		}

		~huffman_tree() { delete [] nodes_; }

		std::size_t num_symbols() const CINDEX_WARN_UNUSED_RESULT { return num_symbols_; }

		bool is_symbol(ref_type ref) const CINDEX_WARN_UNUSED_RESULT { return ref < num_symbols_; }
		std::size_t symbol(ref_type ref) const CINDEX_WARN_UNUSED_RESULT { return guarded_cast<std::size_t>(ref); }

		ref_type root() CINDEX_WARN_UNUSED_RESULT { return guarded_cast<ref_type>(num_symbols_ * 2 - 2); }
		const ref_type root() const CINDEX_WARN_UNUSED_RESULT { return guarded_cast<ref_type>(num_symbols_ * 2 - 2); }

		ref_type& left(ref_type ref) CINDEX_WARN_UNUSED_RESULT
		{
			assert(!is_symbol(ref) && ref < num_symbols_ * 2 - 1);
			return nodes_[(ref - guarded_cast<ref_type>(num_symbols_)) * 2];
		}
		const ref_type& left(ref_type ref) const CINDEX_WARN_UNUSED_RESULT
		{
			assert(!is_symbol(ref) && ref < num_symbols_ * 2 - 1);
			return nodes_[(ref - guarded_cast<ref_type>(num_symbols_)) * 2];
		}

		ref_type& right(ref_type ref) CINDEX_WARN_UNUSED_RESULT
		{
			assert(!is_symbol(ref) && ref < num_symbols_ * 2 - 1);
			return nodes_[(ref - guarded_cast<ref_type>(num_symbols_)) * 2 + 1];
		}
		const ref_type& right(ref_type ref) const CINDEX_WARN_UNUSED_RESULT
		{
			assert(!is_symbol(ref) && ref < num_symbols_ * 2 - 1);
			return nodes_[(ref - guarded_cast<ref_type>(num_symbols_)) * 2 + 1];
		}

	private:
		std::size_t num_symbols_;
		ref_type* nodes_;
	};

	//! huffman tree generator
	template<typename FreqType = double>
	class huffman_tree_generator
	{
	public:
		typedef FreqType freq_type;

	public:
		huffman_tree_generator(std::size_t num_symbols)
			: num_symbols_(num_symbols)
		{
			freqs_ = new FreqType[num_symbols_ * 2 - 1];
			memset(freqs_, 0, sizeof(FreqType) * (num_symbols_ * 2 - 1));
		}

		~huffman_tree_generator() { delete [] freqs_; }

		std::size_t num_symbols() const CINDEX_WARN_UNUSED_RESULT { return num_symbols_; }

		FreqType& operator[](std::size_t symbol) CINDEX_WARN_UNUSED_RESULT { assert(symbol < num_symbols_); return freqs_[symbol]; }

		const FreqType& operator[](std::size_t symbol) const CINDEX_WARN_UNUSED_RESULT { assert(symbol < num_symbols_); return freqs_[symbol]; }

		template<typename RefType>
		void generate(huffman_tree<RefType>& out_t) const
		{
			assert(out_t.num_symbols() == num_symbols_);

			std::multimap<FreqType, RefType> q;
			RefType ref = 0;
			for (; ref < num_symbols_; ref++)
				q.insert(std::make_pair(freqs_[ref], ref));

			for (; ref < num_symbols_ * 2 - 1; ref++)
			{
				RefType left = (*q.begin()).second;
				q.erase(q.begin());
				RefType right = (*q.begin()).second;
				q.erase(q.begin());

				FreqType freq = freqs_[ref] = freqs_[left] + freqs_[right];
				out_t.left(ref) = left;
				out_t.right(ref) = right;

				q.insert(std::make_pair(freq, ref));
			}
		}

	private:
		std::size_t num_symbols_;
		FreqType* freqs_;
	};

	class huffman_table
	{
	public:
		typedef uint32_t block_type;
		typedef uint8_t length_type;

	public:
		template<typename RefType>
		huffman_table(const huffman_tree<RefType>& t)
		{
			num_symbols_ = t.num_symbols();
			lengths_ = new length_type[num_symbols_];
			codes_ = new block_type*[num_symbols_];
			for (std::size_t symbol = 0; symbol < num_symbols_; symbol++)
				codes_[symbol] = NULL;

			block_type prefix[block_info<block_type>::block_count(num_symbols_)];
			generate(t, t.root(), prefix, 0);
		}

		~huffman_table()
		{
			for (std::size_t symbol = 0; symbol < num_symbols_; symbol++)
				if (codes_[symbol])
					delete [] codes_[symbol];
			delete [] codes_;
			delete [] lengths_;
		}

		std::size_t num_symbols() const CINDEX_WARN_UNUSED_RESULT { return num_symbols_; }

		std::size_t length(std::size_t symbol) const CINDEX_WARN_UNUSED_RESULT { assert(symbol < num_symbols_); return lengths_[symbol]; }
		const block_type* code(std::size_t symbol) const CINDEX_WARN_UNUSED_RESULT { assert(symbol < num_symbols_); return codes_[symbol]; }

	protected:
		template<typename RefType>
		void generate(const huffman_tree<RefType>& t, RefType p, block_type* prefix, std::size_t depth)
		{
			if (t.is_symbol(p))
			{
				std::size_t symbol = t.symbol(p);
				assert(depth < (1 << block_info<length_type>::bits_per_block));
				lengths_[symbol] = static_cast<length_type>(depth);
				codes_[symbol] = new block_type[block_info<block_type>::block_count(depth)];
				memcpy(codes_[symbol], prefix, block_info<block_type>::size(depth));
			}
			else
			{
				bit_access::unset(prefix, depth);
				generate(t, t.left(p), prefix, depth + 1);
				bit_access::set(prefix, depth);
				generate(t, t.right(p), prefix, depth + 1);
			}
		}

	private:
		std::size_t num_symbols_;
		length_type* lengths_;
		block_type** codes_;
	};

	template<typename RefType = uint8_t>
	class huffman_rev_table
	{
	public:
		typedef RefType ref_type;
		typedef uint8_t length_type;

	public:
		huffman_rev_table(const huffman_tree<ref_type>& t)
			: max_length_(0)
		{
			assert(t.num_symbols() <= (static_cast<size_t>(1) << block_info<ref_type>::bits_per_block));
			find_max_length(t, t.root(), 0);
			assert(max_length_ <= (1 << block_info<length_type>::bits_per_block));

			//std::cout << max_length_ << std::endl;
			refs_ = new ref_type[1 << max_length_];
			lengths_ = new length_type[1 << max_length_];
			fill_table(t, t.root(), 0, 0);
		}

		~huffman_rev_table()
		{
			delete [] refs_;
			delete [] lengths_;
		}

		std::size_t max_length() const CINDEX_WARN_UNUSED_RESULT { return max_length_; }

		ref_type symbol(std::size_t v) const CINDEX_WARN_UNUSED_RESULT { assert(v < (1u << max_length_)); return refs_[v]; }
		std::size_t length(std::size_t v) const CINDEX_WARN_UNUSED_RESULT { assert(v < (1u << max_length_)); return lengths_[v]; }

	protected:
		void find_max_length(const huffman_tree<ref_type>& t, ref_type p, std::size_t depth)
		{
			if (t.is_symbol(p))
			{
				if (max_length_ < depth)
					max_length_ = depth;
			}
			else
			{
				find_max_length(t, t.left(p), depth + 1);
				find_max_length(t, t.right(p), depth + 1);
			}
		}

		void fill_table(const huffman_tree<ref_type>& t, ref_type p, std::size_t numeric_prefix, std::size_t depth)
		{
			if (t.is_symbol(p))
			{
				std::size_t padding = max_length_ - depth;
				std::size_t base = numeric_prefix << padding;
				for (std::size_t k = 0; k < (1u << padding); k++)
				{
					refs_[base + k] = p;
					assert(depth < (1 << block_info<length_type>::bits_per_block));
					lengths_[base + k] = static_cast<length_type>(depth);
				}
			}
			else
			{
				fill_table(t, t.left(p), (numeric_prefix << 1), depth + 1);
				fill_table(t, t.right(p), (numeric_prefix << 1) | 1, depth + 1);
			}
		}

	private:
		std::size_t max_length_;
		ref_type* refs_;
		length_type* lengths_;
	};

	template<typename RefType = uint8_t>
	class huffman
	{
	public:
		typedef RefType ref_type;
		typedef huffman_table::block_type block_type;

	public:
		static const std::size_t nsymbol = std::size_t(-1);

	public:
		huffman(const huffman_tree<ref_type>& t) : t_(t), tbl_(t)/*, rev_tbl_(t)*/ {}

		template<typename BufferType>
		void encode(BufferType& out_buf, std::size_t symbol) const
		{
			std::size_t len = tbl_.length(symbol);
			const block_type* code = tbl_.code(symbol);

			//for (std::size_t i = 0; i < len; i++)
			//	out_buf.push_back(bit_access::get(code, i));
			out_buf.append(code, 0, len);
		}

		template<typename BufferType>
		std::size_t decode(const BufferType& in_buf, std::size_t& in_out_buf_iter) const
		{
			if (in_out_buf_iter == in_buf.size())
				return nsymbol;

			// simple decoding is ~10% faster than table-based decoding for small codewords
			ref_type p = t_.root();
			while (!t_.is_symbol(p))
			{
				assert(in_out_buf_iter < in_buf.size());

				if (!in_buf[in_out_buf_iter++])
					p = t_.left(p);
				else
					p = t_.right(p);
			}
			return t_.symbol(p);

			/*
			std::size_t fetch_length = rev_tbl_.max_length();
			std::size_t padding = 0;

			if (fetch_length > in_buf.size() - in_out_buf_iter)
			{
				padding = fetch_length - (in_buf.size() - in_out_buf_iter);
				fetch_length -= padding;
			}

			std::size_t idx = in_buf.template get<std::size_t>(in_out_buf_iter, fetch_length);
			idx <<= padding;

			in_out_buf_iter += rev_tbl_.length(idx);
			return rev_tbl_.symbol(idx);
			*/
		}

	private:
		const huffman_tree<ref_type> t_;
		const huffman_table tbl_;
		//const huffman_rev_table<ref_type> rev_tbl_;
	};
}

