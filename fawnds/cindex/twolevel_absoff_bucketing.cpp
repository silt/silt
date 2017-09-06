#include "twolevel_absoff_bucketing.hpp"
#include <iostream>

namespace cindex
{
	template<typename ValueType, typename UpperValueType>
	twolevel_absoff_bucketing<ValueType, UpperValueType>::twolevel_absoff_bucketing(std::size_t size, std::size_t keys_per_bucket, std::size_t keys_per_block)
	{
		resize(size, keys_per_bucket, keys_per_block);
	}

	template<typename ValueType, typename UpperValueType>
	void
	twolevel_absoff_bucketing<ValueType, UpperValueType>::resize(std::size_t size, std::size_t keys_per_bucket, std::size_t keys_per_block CINDEX_UNUSED)
	{
		size_ = size;
		keys_per_bucket_ = keys_per_bucket;

		std::size_t bits_per_key_bound = 4;
		upper_bucket_size_ = (std::size_t(1) << (sizeof(ValueType) * 8)) / (bits_per_key_bound * keys_per_bucket_);
		if (upper_bucket_size_ == 0)
			upper_bucket_size_ = 1;

		bucket_info_.resize(size);
		upper_bucket_info_.resize(size / upper_bucket_size_ + 2);
		upper_bucket_info_[0][0] = 0;
		upper_bucket_info_[0][1] = 0;
		current_i_ = 0;

		//if (size_)
		//{
		//	std::cout << "bucket_count: " << size << std::endl;
		//	std::cout << "upper_bucket_count: " << size / upper_bucket_size_ + 2 << std::endl;
		//}
	}

	template<typename ValueType, typename UpperValueType>
	void
	twolevel_absoff_bucketing<ValueType, UpperValueType>::serialize(serializer& s) const
	{
		s << size_;
		s << keys_per_bucket_;
		s << upper_bucket_size_;
		for (std::size_t i = 0; i < size_; i++)
			s << bucket_info_[i][0] << bucket_info_[i][1];
		for (std::size_t i = 0; i < size_ / upper_bucket_size_ + 2; i++)
			s << upper_bucket_info_[i][0] << upper_bucket_info_[i][1];
		s << current_i_;
	}

	template<typename ValueType, typename UpperValueType>
	void
	twolevel_absoff_bucketing<ValueType, UpperValueType>::deserialize(serializer& s)
	{
		s >> size_;
		s >> keys_per_bucket_;
		s >> upper_bucket_size_;
		bucket_info_.resize(size_);
		for (std::size_t i = 0; i < size_; i++)
			s >> bucket_info_[i][0] >> bucket_info_[i][1];
		upper_bucket_info_.resize(size_ / upper_bucket_size_ + 2);
		for (std::size_t i = 0; i < size_ / upper_bucket_size_ + 2; i++)
			s >> upper_bucket_info_[i][0] >> upper_bucket_info_[i][1];
		s >> current_i_;
	}

	template<typename ValueType, typename UpperValueType>
	void
	twolevel_absoff_bucketing<ValueType, UpperValueType>::insert(const std::size_t& index_offset, const std::size_t& dest_offset)
	{
		assert(current_i_ < size_);

		std::size_t base_index_offset = upper_index_offset(current_i_ / upper_bucket_size_);
		std::size_t base_dest_offset = upper_dest_offset(current_i_ / upper_bucket_size_);

		//std::cout << "i: " << current_i_ << std::endl;
		//std::cout << "base: " << base_index_offset << " " << base_dest_offset << std::endl;
		//std::cout << "current: " << index_offset << " " << dest_offset << std::endl;

		bucket_info_[current_i_][0] = guarded_cast<value_type>(index_offset - base_index_offset);
		bucket_info_[current_i_][1] = guarded_cast<value_type>(dest_offset - base_dest_offset);
		upper_bucket_info_[current_i_ / upper_bucket_size_ + 1][0] = guarded_cast<upper_value_type>(index_offset);
		upper_bucket_info_[current_i_ / upper_bucket_size_ + 1][1] = guarded_cast<upper_value_type>(dest_offset);

		assert(this->index_offset(current_i_) == index_offset);
		assert(this->dest_offset(current_i_) == dest_offset);

		current_i_++;
	}

	template<typename ValueType, typename UpperValueType>
	std::size_t
	twolevel_absoff_bucketing<ValueType, UpperValueType>::index_offset(std::size_t i) const
	{
		assert(i < size_);
		return upper_index_offset(i / upper_bucket_size_) + bucket_info_[i][0];
	}

	template<typename ValueType, typename UpperValueType>
	std::size_t
	twolevel_absoff_bucketing<ValueType, UpperValueType>::dest_offset(std::size_t i) const
	{
		assert(i < size_);
		return upper_dest_offset(i / upper_bucket_size_) + bucket_info_[i][1];
	}

	template<typename ValueType, typename UpperValueType>
	std::size_t
	twolevel_absoff_bucketing<ValueType, UpperValueType>::upper_index_offset(std::size_t i) const
	{
		assert(i < size_ / upper_bucket_size_ + 1);
		return upper_bucket_info_[i][0];
	}

	template<typename ValueType, typename UpperValueType>
	std::size_t
	twolevel_absoff_bucketing<ValueType, UpperValueType>::upper_dest_offset(std::size_t i) const
	{
		assert(i < size_ / upper_bucket_size_ + 1);
		return upper_bucket_info_[i][1];
	}

	template<typename ValueType, typename UpperValueType>
	std::size_t
	twolevel_absoff_bucketing<ValueType, UpperValueType>::size() const
	{
		return size_;
	}

	template<typename ValueType, typename UpperValueType>
	std::size_t
	twolevel_absoff_bucketing<ValueType, UpperValueType>::bit_size() const
	{
		std::size_t bit_size = bucket_info_.size() * 2 * sizeof(value_type) * 8;
		bit_size += upper_bucket_info_.size() * 2 * sizeof(upper_value_type) * 8;
		return bit_size;
	}

	template class twolevel_absoff_bucketing<>;
}

