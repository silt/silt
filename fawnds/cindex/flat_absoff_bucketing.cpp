#include "flat_absoff_bucketing.hpp"
#include <iostream>

namespace cindex
{
	template<typename ValueType>
	flat_absoff_bucketing<ValueType>::flat_absoff_bucketing(std::size_t size, std::size_t keys_per_bucket, std::size_t keys_per_block CINDEX_UNUSED)
	{
		resize(size, keys_per_bucket, keys_per_block);
	}

	template<typename ValueType>
	void
	flat_absoff_bucketing<ValueType>::resize(std::size_t size, std::size_t keys_per_bucket CINDEX_UNUSED, std::size_t keys_per_block CINDEX_UNUSED)
	{
		size_ = size;

		bucket_info_.resize(size);
		current_i_ = 0;

		//if (size_)
		//	std::cout << "bucket_count: " << size << std::endl;
	}

	template<typename ValueType>
	void
	flat_absoff_bucketing<ValueType>::serialize(serializer& s) const
	{
		s << size_;
		for (std::size_t i = 0; i < size_; i++)
			s << bucket_info_[i][0] << bucket_info_[i][1];
		s << current_i_;
	}

	template<typename ValueType>
	void
	flat_absoff_bucketing<ValueType>::deserialize(serializer& s)
	{
		s >> size_;
		bucket_info_.resize(size_);
		for (std::size_t i = 0; i < size_; i++)
			s >> bucket_info_[i][0] >> bucket_info_[i][1];
		s >> current_i_;
	}

	template<typename ValueType>
	void
	flat_absoff_bucketing<ValueType>::insert(const std::size_t& index_offset, const std::size_t& dest_offset)
	{
		assert(current_i_ < size_);

		bucket_info_[current_i_][0] = guarded_cast<value_type>(index_offset);
		bucket_info_[current_i_][1] = guarded_cast<value_type>(dest_offset);

		assert(this->index_offset(current_i_) == index_offset);
		assert(this->dest_offset(current_i_) == dest_offset);

		current_i_++;
	}

	template<typename ValueType>
	std::size_t
	flat_absoff_bucketing<ValueType>::index_offset(std::size_t i) const
	{
		assert(i < size_);
		return bucket_info_[i][0];
	}

	template<typename ValueType>
	std::size_t
	flat_absoff_bucketing<ValueType>::dest_offset(std::size_t i) const
	{
		assert(i < size_);
		return bucket_info_[i][1];
	}

	template<typename ValueType>
	std::size_t
	flat_absoff_bucketing<ValueType>::size() const
	{
		return size_;
	}

	template<typename ValueType>
	std::size_t
	flat_absoff_bucketing<ValueType>::bit_size() const
	{
		return bucket_info_.size() * 2 * sizeof(value_type) * 8;
	}

	template class flat_absoff_bucketing<>;
}

