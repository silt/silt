#pragma once

#include "common.hpp"
#include <vector>
#include <boost/array.hpp>

namespace cindex
{
	template<typename ValueType = uint32_t>
	class flat_absoff_bucketing
	{
	public:
		typedef ValueType value_type;

	public:
		flat_absoff_bucketing(std::size_t size = 0, std::size_t keys_per_bucket = 1, std::size_t keys_per_block = 1);

		void resize(std::size_t size, std::size_t keys_per_bucket, std::size_t keys_per_block);

		void insert(const std::size_t& index_offset, const std::size_t& dest_offset);
		void finalize() {}

		std::size_t index_offset(std::size_t i) const CINDEX_WARN_UNUSED_RESULT;
		std::size_t dest_offset(std::size_t i) const CINDEX_WARN_UNUSED_RESULT;

		std::size_t size() const CINDEX_WARN_UNUSED_RESULT;

		std::size_t bit_size() const CINDEX_WARN_UNUSED_RESULT;

	private:
		std::size_t size_;

		std::vector<boost::array<value_type, 2> > bucket_info_;
		std::size_t current_i_;
	};
}

