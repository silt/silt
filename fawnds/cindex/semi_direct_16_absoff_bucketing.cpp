#include "semi_direct_16_absoff_bucketing.hpp"
#include <iostream>

namespace cindex
{
	const std::size_t semi_direct_16_absoff_bucketing::ranks_[16] = {
		0 /* unused */, 0, 1, 2,
		0 /* unused */, 3, 4, 5,
		0 /* unused */, 6, 7, 8,
		0 /* unused */, 9, 10, 11
	};

	semi_direct_16_absoff_bucketing::semi_direct_16_absoff_bucketing(std::size_t size, std::size_t keys_per_bucket, std::size_t keys_per_block)
	{
		resize(size, keys_per_bucket, keys_per_block);
	}

	void
	semi_direct_16_absoff_bucketing::resize(std::size_t size, std::size_t keys_per_bucket CINDEX_UNUSED, std::size_t keys_per_block CINDEX_UNUSED)
	{
		if (size % 16 != 0)
			size = (size + 15) / 16 * 16;
		size_ = size;

		boost::array<uint32_t, 2> v;
		v[0] = v[1] = 0u;
		bucket_info_.resize(0);
		bucket_info_.resize(size / 16 * 5, v);
		current_i_ = 0;

		//if (size_)
		//	std::cout << "bucket_count: " << size << std::endl;
		if (keys_per_bucket > 128)
			std::cout << "warning: too high keys per bucket" << std::endl;
	}

	void
	semi_direct_16_absoff_bucketing::serialize(serializer& s) const
	{
		s << size_;
		for (std::size_t i = 0; i < size_ / 16 * 5; i++)
			s << bucket_info_[i][0] << bucket_info_[i][1];
		s << current_i_;
		for (std::size_t i = 0; i < 16; i++)
			s << last_index_offsets_[i];
		for (std::size_t i = 0; i < 16; i++)
			s << last_dest_offsets_[i];
	}

	void
	semi_direct_16_absoff_bucketing::deserialize(serializer& s)
	{
		s >> size_;
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
	}

	void
	semi_direct_16_absoff_bucketing::store(const std::size_t& idx, const std::size_t& type, const std::size_t& mask, const std::size_t& shift, const std::size_t& v)
	{
		assert((v & mask) == v);	// overflow test
		(void)mask;
		bucket_info_[idx][type] |= guarded_cast<uint32_t>(v << shift);
	}

	std::size_t
	semi_direct_16_absoff_bucketing::load(const std::size_t& idx, const std::size_t& type, const std::size_t& mask, const std::size_t& shift) const
	{
		return (bucket_info_[idx][type] >> shift) & mask;
	}

	void
	semi_direct_16_absoff_bucketing::insert(const std::size_t& index_offset, const std::size_t& dest_offset)
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
			std::size_t index_offset_delta = index_offset - last_index_offsets_[diff_base % 16];
			std::size_t dest_offset_delta = dest_offset - last_dest_offsets_[diff_base % 16];

			store(base_idx + 1, 0, 0x3ff, ((current_i_ % 16) / 4 - 1) * 10,
					index_offset_delta);
			store(base_idx + 1, 1, 0x3ff, ((current_i_ % 16) / 4 - 1) * 10,
					dest_offset_delta);
		}
		else
		{
			std::size_t diff_base = current_i_ - 1;
			std::size_t index_offset_delta = index_offset - last_index_offsets_[diff_base % 16];
			std::size_t dest_offset_delta = dest_offset - last_dest_offsets_[diff_base % 16];
			std::size_t rank = ranks_[current_i_ % 16];

			store(base_idx + 2 + rank / 4, 0, 0xff, rank % 4 * 8,
					index_offset_delta);
			store(base_idx + 2 + rank / 4, 1, 0xff, rank % 4 * 8,
					dest_offset_delta);
		}

		assert(this->index_offset(current_i_) == index_offset);
		assert(this->dest_offset(current_i_) == dest_offset);

		last_index_offsets_[current_i_ % 16] = index_offset;
		last_dest_offsets_[current_i_ % 16] = dest_offset;

		current_i_++;
	}

	std::size_t
	semi_direct_16_absoff_bucketing::index_offset(std::size_t i) const
	{
		assert(i < size_);

		std::size_t base_idx = i / 16 * 5;
		std::size_t v;
		if (i % 16 == 0)
			v = load(base_idx + 0, 0, 0xffffffff, 0);
		else if (i % 4 == 0)
		{
			std::size_t diff_base = i - 4;
			v = load(base_idx + 1, 0, 0x3ff, ((i % 16) / 4 - 1) * 10) + index_offset(diff_base);
		}
		else
		{
			std::size_t diff_base = i - 1;
			std::size_t rank = ranks_[i % 16];
			v = load(base_idx + 2 + rank / 4, 0, 0xff, rank % 4 * 8) + index_offset(diff_base);
		}

		//std::cout << "index_offset(" << i << ") = " << v << std::endl;
		return v;
	}

	std::size_t
	semi_direct_16_absoff_bucketing::dest_offset(std::size_t i) const
	{
		assert(i < size_);

		std::size_t base_idx = i / 16 * 5;
		std::size_t v;
		if (i % 16 == 0)
			v = load(base_idx + 0, 1, 0xffffffff, 0);
		else if (i % 4 == 0)
		{
			std::size_t diff_base = i - 4;
			v = load(base_idx + 1, 1, 0x3ff, ((i % 16) / 4 - 1) * 10) + dest_offset(diff_base);
		}
		else
		{
			std::size_t diff_base = i - 1;
			std::size_t rank = ranks_[i % 16];
			v = load(base_idx + 2 + rank / 4, 1, 0xff, rank % 4 * 8) + dest_offset(diff_base);
		}

		//std::cout << "dest_offset(" << i << ") = " << v << std::endl;
		return v;
	}

	std::size_t
	semi_direct_16_absoff_bucketing::size() const
	{
		return size_;
	}

	std::size_t
	semi_direct_16_absoff_bucketing::bit_size() const
	{
		return bucket_info_.size() * 2 * sizeof(uint32_t) * 8;
	}
}

