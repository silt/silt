#pragma once

#include "common.hpp"
#include "bit_access.hpp"
#include <cstring>
#include <vector>
#include <cstdio>
#include <nsort.h>

namespace cindex
{
	class quick_sort
	{
	public:
		template<typename KeyArrayType>
		static void sort(KeyArrayType& arr, std::size_t off, std::size_t n)
		{
			assert(off + n <= arr.size());
			sort_rec(arr, off, off + n - 1);
		}

	protected:
		template<typename KeyArrayType>
		static void swap(KeyArrayType& arr, std::size_t i, std::size_t j)
		{
			assert(i < arr.size() && j < arr.size());

			uint8_t temp[arr.key_len()];
			memcpy(temp, arr[i], sizeof(temp));
			memcpy(arr[i], arr[j], sizeof(temp));
			memcpy(arr[j], temp, sizeof(temp));
		}

		template<typename KeyArrayType>
		static int cmp(const KeyArrayType& arr, std::size_t i, std::size_t j)
		{
			assert(i < arr.size() && j < arr.size());
			return memcmp(arr[i], arr[j], arr.key_len());
		}

		template<typename KeyArrayType>
		static void sort_rec(KeyArrayType& arr, std::size_t left, std::size_t right)
		{
			assert(left <= arr.size());
			assert(right < arr.size());

			if (left < right)
			{
				std::size_t p = left;
				std::size_t i = left;
				std::size_t j = right + 1;
				while (i < j)
				{
					while (i < right && cmp(arr, ++i, p) <= 0);
					while (left < j && cmp(arr, p, --j) <= 0);
					if (i < j)
						swap(arr, i, j);
				}
				if (p != j)
					swap(arr, p, j);

				if (j > 0)	// we are using unsigned type, so let's avoid negative numbers
					sort_rec(arr, left, j - 1);
				sort_rec(arr, j + 1, right);
			}
		}
	};

	class indirect_radix_sort
	{
	public:
		template<typename KeyArrayType, typename IndexType>
		static void init(const KeyArrayType& arr, std::vector<IndexType>& indices)
		{
			indices.clear();
			indices.reserve(arr.size());
			for (std::size_t i = 0; i < arr.size(); i++)
				indices.push_back(guarded_cast<IndexType>(i));
		}

		template<typename KeyArrayType, typename IndexType>
		static void cluster(const KeyArrayType& arr, const std::vector<IndexType>& indices, std::vector<std::vector<IndexType> >& buckets, std::size_t bucket_bits, std::size_t skip_bits = 0)
		{
			assert(indices.size() == arr.size());
			assert((skip_bits + bucket_bits) <= arr.key_len() * 8);

			std::size_t num_buckets = static_cast<std::size_t>(1) << bucket_bits;

			buckets.clear();
			buckets.resize(num_buckets);

			for (std::size_t i = 0; i < arr.size(); i++)
			{
				std::size_t b = 0;
				assert(indices[i] < arr.size());
				bit_access::copy_set(&b, block_info<std::size_t>::bits_per_block - bucket_bits,
									 arr[indices[i]], skip_bits, bucket_bits);
				assert(b < num_buckets);
				buckets[b].push_back(guarded_cast<IndexType>(indices[i]));
			}
		}

		template<typename KeyArrayType, typename IndexType>
		static void collect(const KeyArrayType& arr, std::vector<IndexType>& indices, std::vector<std::vector<IndexType> >& buckets)
		{
			indices.clear();
			indices.reserve(arr.size());
			for (std::size_t b = 0; b < buckets.size(); b++)
				indices.insert(indices.end(), buckets[b].begin(), buckets[b].end());
			assert(indices.size() == arr.size());
		}
	};

	struct merge_sort
	{
	public:
		template<typename InputType, typename OutputType, typename Comparer>
		static void merge(const InputType inputs[], std::size_t input_size, std::size_t input_count, OutputType& output, Comparer comp = Comparer())
		{
			// TODO: implement with min-heap
		}
	};

	struct nsort
	{
	public:
		template<typename InputType, typename OutputType>
		static void sort(const InputType& input, std::size_t input_size, std::size_t input_count, OutputType& output, std::size_t memory, std::size_t processes)
		{
			nsort_msg_t ret;
			nsort_t ctx;

			char sortdef[1024];
			snprintf(sortdef, sizeof(sortdef),
						" -memory=%zu"
						" -processes=%zu"
						" -format=size: %zu"
					 	" -field=Key, char, offset: 0, size: %zu"
					 	" -key=Key",
				memory, processes, input_size, input_size);

			ret = nsort_define(sortdef, 0, NULL, &ctx);
			assert(ret == NSORT_SUCCESS);

			for (std::size_t i = 0; i < input_count; i++)
			{
				ret = nsort_release_recs(input(), input_size, &ctx);
				assert(ret == NSORT_SUCCESS);
			}

			ret = nsort_release_end(&ctx);
			assert(ret == NSORT_SUCCESS);

			std::size_t output_count = 0;
			uint8_t buf[input_size];
			while (true)
			{
				std::size_t size = input_size;
				ret = nsort_return_recs(buf, &size, &ctx);
				if (ret == NSORT_END_OF_OUTPUT)
					break;
				assert(ret == NSORT_SUCCESS);
				assert(size == input_size);
				output(buf);
				output_count++;
			}

			assert(input_count == output_count);

			ret = nsort_end(&ctx);
			assert(ret == NSORT_SUCCESS);
		}
	};
}

