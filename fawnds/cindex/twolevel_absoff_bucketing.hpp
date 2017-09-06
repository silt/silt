#pragma once

#include "common.hpp"
#include "serializer.hpp"
#include <vector>
#include <boost/array.hpp>

namespace cindex
{
	template<typename ValueType = uint16_t, typename UpperValueType = uint32_t>
	class twolevel_absoff_bucketing
	{
	public:
		typedef ValueType value_type;
		typedef UpperValueType upper_value_type;

	public:
		twolevel_absoff_bucketing(std::size_t size = 0, std::size_t keys_per_bucket = 1, std::size_t keys_per_block = 1);

		void resize(std::size_t size, std::size_t keys_per_bucket, std::size_t keys_per_block);

		void serialize(serializer& s) const;
		void deserialize(serializer& s);

		void insert(const std::size_t& index_offset, const std::size_t& dest_offset);
		void finalize() {}

		std::size_t index_offset(std::size_t i) const CINDEX_WARN_UNUSED_RESULT;
		std::size_t dest_offset(std::size_t i) const CINDEX_WARN_UNUSED_RESULT;

		std::size_t size() const CINDEX_WARN_UNUSED_RESULT;

		std::size_t bit_size() const CINDEX_WARN_UNUSED_RESULT;

	protected:
		std::size_t upper_index_offset(std::size_t i) const CINDEX_WARN_UNUSED_RESULT;
		std::size_t upper_dest_offset(std::size_t i) const CINDEX_WARN_UNUSED_RESULT;

	private:
		std::size_t size_;
		std::size_t keys_per_bucket_;

		std::size_t upper_bucket_size_;

		std::vector<boost::array<value_type, 2> > bucket_info_;
		std::vector<boost::array<upper_value_type, 2> > upper_bucket_info_;

		std::size_t current_i_;
	};
}

