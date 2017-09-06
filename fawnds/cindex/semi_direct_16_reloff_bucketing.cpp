#include "semi_direct_16_reloff_bucketing.hpp"
#include "expected_size.hpp"
#include <cstring>
#include <iostream>

#define USE_KEY_DIVERGENCE_FOR_INDEX

namespace cindex
{
	const std::size_t semi_direct_16_reloff_bucketing::ranks_[16] = {
		0 /* unused */, 0, 1, 2,
		0 /* unused */, 3, 4, 5,
		0 /* unused */, 6, 7, 8,
		0 /* unused */, 9, 10, 11
	};

	semi_direct_16_reloff_bucketing::semi_direct_16_reloff_bucketing(std::size_t size, std::size_t keys_per_bucket, std::size_t keys_per_block)
	{
		resize(size, keys_per_bucket, keys_per_block);
		assert(sizeof(std::size_t) == sizeof(long long int));
	}

	void
	semi_direct_16_reloff_bucketing::resize(std::size_t size, std::size_t keys_per_bucket, std::size_t keys_per_block)
	{
		if (size % 16 != 0)
			size = (size + 15) / 16 * 16;
		size_ = size;

		keys_per_bucket_ = keys_per_bucket;
		keys_per_block_ = keys_per_block;
		bits_per_key_ = expected_size::index_size(keys_per_bucket_, keys_per_block_);
		bits_per_bucket_ = static_cast<std::size_t>(static_cast<double>(keys_per_bucket_) * bits_per_key_);
		if (size_)
		{
			std::cout << "expected_bits_per_key_trie_only: " << expected_size::index_size(keys_per_bucket_, keys_per_block_) << std::endl;
			std::cout << "expected_index_offset_delta: " << bits_per_bucket_ << std::endl;
			std::cout << "expected_dest_offset_delta: " << keys_per_bucket_ << std::endl;
		}

		boost::array<uint32_t, 2> v;
		v[0] = v[1] = 0u;
		bucket_info_.resize(0);
		bucket_info_.resize(size / 16 * 5, v);
		current_i_ = 0;

		//if (size_)
		//	std::cout << "bucket_count: " << size << std::endl;
		//if (keys_per_bucket_ > 256)
		//	std::cout << "warning: too high keys per bucket" << std::endl;
	}

	void
	semi_direct_16_reloff_bucketing::serialize(serializer& s) const
	{
		s << size_;
		s << keys_per_bucket_;
		s << keys_per_block_;
		s << bits_per_key_;
		s << bits_per_bucket_;
		for (std::size_t i = 0; i < size_ / 16 * 5; i++)
			s << bucket_info_[i][0] << bucket_info_[i][1];
		s << current_i_;
		for (std::size_t i = 0; i < 16; i++)
			s << last_index_offsets_[i];
		for (std::size_t i = 0; i < 16; i++)
			s << last_dest_offsets_[i];
		std::size_t size = overflow_.size();
		s << size;
		for (overflow_table::const_iterator it = overflow_.begin(); it != overflow_.end(); ++it)
			s << (*it).first << (*it).second;
	}

	void
	semi_direct_16_reloff_bucketing::deserialize(serializer& s)
	{
		s >> size_;
		s >> keys_per_bucket_;
		s >> keys_per_block_;
		s >> bits_per_key_;
		s >> bits_per_bucket_;
		boost::array<uint32_t, 2> v;
		v[0] = v[1] = 0u;
		bucket_info_.resize(0);
		bucket_info_.resize(size_ / 16 * 5, v);
		for (std::size_t i = 0; i < size_ / 16 * 5; i++)
			s >> bucket_info_[i][0] >> bucket_info_[i][1];
		s >> current_i_;
		for (std::size_t i = 0; i < 16; i++)
			s >> last_index_offsets_[i];
		for (std::size_t i = 0; i < 16; i++)
			s >> last_dest_offsets_[i];
		std::size_t size;
		s >> size;
		overflow_.clear();
		for (std::size_t i = 0; i < size; i++)
		{
			overflow_table::key_type key;
			overflow_table::mapped_type data;
			s >> key >> data;
			overflow_.insert(std::make_pair(key, data));
		}
	}

	void
	semi_direct_16_reloff_bucketing::store(const std::size_t& idx, const std::size_t& type, const std::size_t& mask, const std::size_t& shift, const std::size_t& v)
	{
		assert((v & mask) == v);
		(void)mask;
		bucket_info_[idx][type] |= guarded_cast<uint32_t>(v << shift);
	}

	std::size_t
	semi_direct_16_reloff_bucketing::load(const std::size_t& idx, const std::size_t& type, const std::size_t& mask, const std::size_t& shift) const
	{
		return (bucket_info_[idx][type] >> shift) & mask;
	}

	void
	semi_direct_16_reloff_bucketing::store_delta(const std::size_t& idx, const std::size_t& type, const std::size_t& mask, const std::size_t& shift, const std::size_t& id, const long long int& v)
	{
		long long int biased_v = v + guarded_cast<long long int>(mask / 2);
		if (biased_v < 0 || biased_v >= guarded_cast<long long int>(mask) - 1)
		{
			// overflow
			//std::cout << "overflow: id: " << id << " v: " << v << " biased_v: " << biased_v << std::endl;
			assert(current_i_ < 1000 || overflow_.size() < current_i_ * 2 / 5);	// allow up to 20 %

			std::pair<overflow_table::iterator, bool> ret = overflow_.insert(std::make_pair(id, guarded_cast<overflow_table::mapped_type>(v)));
			assert(ret.second);	// no duplicate entry
			store(idx, type, mask, shift, mask);
		}
		else
		{
			std::size_t stored_biased_v = guarded_cast<std::size_t>(biased_v);
			store(idx, type, mask, shift, stored_biased_v);
		}
	}

	long long int
	semi_direct_16_reloff_bucketing::load_delta(const std::size_t& idx, const std::size_t& type, const std::size_t& mask, const std::size_t& shift, const std::size_t& id) const
	{
		std::size_t loaded_biased_v = load(idx, type, mask, shift);
		if (loaded_biased_v == mask)
		{
			overflow_table::const_iterator it = overflow_.find(id);
			assert(it != overflow_.end());
			return (*it).second;
		}
		else
			return guarded_cast<long long int>(loaded_biased_v) - guarded_cast<long long int>(mask / 2);
	}

	void
	semi_direct_16_reloff_bucketing::insert(const std::size_t& index_offset, const std::size_t& dest_offset)
	{
		assert(current_i_ < size_);

		//std::cout << "i: " << current_i_ << std::endl;
		//std::cout << "current: " << index_offset << " " << dest_offset << std::endl;

		std::size_t base_idx = current_i_ / 16 * 5;
		if (current_i_ % 16 == 0)
		{
			store(base_idx + 0, 0, 0xffffffff, 0, index_offset);
			store(base_idx + 0, 1, 0xffffffff, 0, dest_offset);
		}
		else if (current_i_ % 4 == 0)
		{
			std::size_t diff_base = current_i_ - 4;
			long long int index_offset_delta = guarded_cast<long long int>(index_offset - last_index_offsets_[diff_base % 16]) - guarded_cast<long long int>(4 * bits_per_bucket_);
			long long int dest_offset_delta = guarded_cast<long long int>(dest_offset - last_dest_offsets_[diff_base % 16]) - guarded_cast<long long int>(4 * keys_per_bucket_);

#ifdef USE_KEY_DIVERGENCE_FOR_INDEX
			index_offset_delta -= static_cast<long long int>(static_cast<double>(dest_offset_delta) * bits_per_key_);
#endif

			store_delta(base_idx + 1, 0, 0x3ff, ((current_i_ % 16) / 4 - 1) * 10,
						current_i_ * 2 + 0, index_offset_delta);
			store_delta(base_idx + 1, 1, 0x3ff, ((current_i_ % 16) / 4 - 1) * 10,
						current_i_ * 2 + 1, dest_offset_delta);
		}
		else
		{
			std::size_t diff_base = current_i_ - 1;
			long long int index_offset_delta = guarded_cast<long long int>(index_offset - last_index_offsets_[diff_base % 16]) - guarded_cast<long long int>(1 * bits_per_bucket_);
			long long int dest_offset_delta = guarded_cast<long long int>(dest_offset - last_dest_offsets_[diff_base % 16]) - guarded_cast<long long int>(1 * keys_per_bucket_);
			std::size_t rank = ranks_[current_i_ % 16];

#ifdef USE_KEY_DIVERGENCE_FOR_INDEX
			index_offset_delta -= static_cast<long long int>(static_cast<double>(dest_offset_delta) * bits_per_key_);
#endif

			store_delta(base_idx + 2 + rank / 4, 0, 0xff, rank % 4 * 8,
						current_i_ * 2 + 0, index_offset_delta);
			store_delta(base_idx + 2 + rank / 4, 1, 0xff, rank % 4 * 8,
						current_i_ * 2 + 1, dest_offset_delta);
		}

		assert(this->index_offset(current_i_) == index_offset);
		assert(this->dest_offset(current_i_) == dest_offset);

		last_index_offsets_[current_i_ % 16] = index_offset;
		last_dest_offsets_[current_i_ % 16] = dest_offset;

		current_i_++;
	}

	void
	semi_direct_16_reloff_bucketing::finalize()
	{
		std::cout << "overflow: " << static_cast<double>(overflow_.size()) / static_cast<double>(current_i_ * 2) << std::endl;
	}

	std::size_t
	semi_direct_16_reloff_bucketing::index_offset(std::size_t i) const
	{
		assert(i < size_);

		std::size_t base_idx = i / 16 * 5;
		std::size_t v;
		if (i % 16 == 0)
			v = load(base_idx + 0, 0, 0xffffffff, 0);
		else if (i % 4 == 0)
		{
			std::size_t diff_base = i - 4;
			v = load_delta(base_idx + 1, 0, 0x3ff, ((i % 16) / 4 - 1) * 10, i * 2 + 0) + index_offset(diff_base) + 4 * bits_per_bucket_;

#ifdef USE_KEY_DIVERGENCE_FOR_INDEX
			long long int dest_offset_delta = load_delta(base_idx + 1, 1, 0x3ff, ((i % 16) / 4 - 1) * 10, i * 2 + 0);
			v += static_cast<std::size_t>(static_cast<long long int>(static_cast<double>(dest_offset_delta) * bits_per_key_));
#endif
		}
		else
		{
			std::size_t diff_base = i - 1;
			std::size_t rank = ranks_[i % 16];
			v = load_delta(base_idx + 2 + rank / 4, 0, 0xff, rank % 4 * 8, i * 2 + 0) + index_offset(diff_base) + 1 * bits_per_bucket_;

#ifdef USE_KEY_DIVERGENCE_FOR_INDEX
			long long int dest_offset_delta = load_delta(base_idx + 2 + rank / 4, 1, 0xff, rank % 4 * 8, i * 2 + 1);
			v += static_cast<std::size_t>(static_cast<long long int>(static_cast<double>(dest_offset_delta) * bits_per_key_));
#endif
		}

		//std::cout << "index_offset(" << i << ") = " << v << std::endl;
		return v;
	}

	std::size_t
	semi_direct_16_reloff_bucketing::dest_offset(std::size_t i) const
	{
		assert(i < size_);

		std::size_t base_idx = i / 16 * 5;
		std::size_t v;
		if (i % 16 == 0)
			v = load(base_idx + 0, 1, 0xffffffff, 0);
		else if (i % 4 == 0)
		{
			std::size_t diff_base = i - 4;
			v = load_delta(base_idx + 1, 1, 0x3ff, ((i % 16) / 4 - 1) * 10, i * 2 + 1) + dest_offset(diff_base) + 4 * keys_per_bucket_;
		}
		else
		{
			std::size_t diff_base = i - 1;
			std::size_t rank = ranks_[i % 16];
			v = load_delta(base_idx + 2 + rank / 4, 1, 0xff, rank % 4 * 8, i * 2 + 1) + dest_offset(diff_base) + 1 * keys_per_bucket_;
		}

		//std::cout << "dest_offset(" << i << ") = " << v << std::endl;
		return v;
	}

	std::size_t
	semi_direct_16_reloff_bucketing::size() const
	{
		return size_;
	}

	std::size_t
	semi_direct_16_reloff_bucketing::bit_size() const
	{
		std::size_t bit_size = bucket_info_.size() * 2 * sizeof(uint32_t) * 8;
		// TODO: this assumes a non-chaining hash table with 50% utilization,
		// which is not very true for most std::tr1::unordered_map implementations 
		bit_size += (sizeof(overflow_table::key_type) + sizeof(overflow_table::mapped_type)) * 8 * overflow_.size() * 2;
		return bit_size;
	}
}

