#pragma once

#include "common.hpp"
#include <vector>
#include <boost/array.hpp>

namespace cindex
{
	class semi_direct_16_absoff_bucketing
	{
	public:
		semi_direct_16_absoff_bucketing(std::size_t size = 0, std::size_t keys_per_bucket = 1, std::size_t keys_per_block = 1);

		void resize(std::size_t size, std::size_t keys_per_bucket, std::size_t keys_per_block);

		void insert(const std::size_t& index_offset, const std::size_t& dest_offset);
		void finalize() {}

		std::size_t index_offset(std::size_t i) const CINDEX_WARN_UNUSED_RESULT;
		std::size_t dest_offset(std::size_t i) const CINDEX_WARN_UNUSED_RESULT;

		std::size_t size() const CINDEX_WARN_UNUSED_RESULT;

		std::size_t bit_size() const CINDEX_WARN_UNUSED_RESULT;

	protected:
		void store(const std::size_t& idx, const std::size_t& type, const std::size_t& mask, const std::size_t& shift, const std::size_t& v);
		std::size_t load(const std::size_t& idx, const std::size_t& type, const std::size_t& mask, const std::size_t& shift) const;

	private:
		std::size_t size_;

		std::vector<boost::array<uint32_t, 2> > bucket_info_;
		std::size_t current_i_;

		std::size_t last_index_offsets_[16];
		std::size_t last_dest_offsets_[16];

		static const std::size_t ranks_[16];
	};
}

