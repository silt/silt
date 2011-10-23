#include "twolevel_reloff_bucketing.hpp"
#include "expected_size.hpp"
#include <iostream>
#include <gsl/gsl_cdf.h>

#define USE_KEY_DIVERGENCE_FOR_INDEX

namespace cindex
{
	twolevel_reloff_bucketing::twolevel_reloff_bucketing(std::size_t size, std::size_t keys_per_bucket, std::size_t keys_per_block)
	{
		resize(size, keys_per_bucket, keys_per_block);
		assert(sizeof(std::size_t) == sizeof(long long int));
	}

	void
	twolevel_reloff_bucketing::resize(std::size_t size, std::size_t keys_per_bucket, std::size_t keys_per_block)
	{
		if (size % group_size_ != 0)
			size = (size + group_size_ - 1) / group_size_ * group_size_;
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

		boost::array<uint8_t, 2> v;
		v[0] = v[1] = 0u;
		bucket_info_.resize(0);
		bucket_info_.resize(size_ / group_size_ * (4 + (group_size_ - 1)), v);
		current_i_ = 0;

		//if (size_)
		//	std::cout << "bucket_count: " << size << std::endl;
		//if (keys_per_bucket_ > 256)
		//	std::cout << "warning: too high keys per bucket" << std::endl;
		
		if (size_)
		{
			double c = 0.;

			if (bits_per_bucket_ > 0)
			{
				if (bits_per_bucket_ > 127)
					c += gsl_cdf_poisson_P(guarded_cast<unsigned int>(bits_per_bucket_) - 127, static_cast<double>(bits_per_bucket_));
				c += (1. - gsl_cdf_poisson_P(guarded_cast<unsigned int>(bits_per_bucket_) + 127, static_cast<double>(bits_per_bucket_)));
			}

			if (keys_per_bucket_ > 0)
			{
				if (keys_per_bucket_ > 127)
					c += gsl_cdf_poisson_P(guarded_cast<unsigned int>(keys_per_bucket_) - 127, static_cast<double>(keys_per_bucket_));
				c += (1. - gsl_cdf_poisson_P(guarded_cast<unsigned int>(keys_per_bucket_) + 127, static_cast<double>(keys_per_bucket_)));
			}

			std::cout << "expected_overflow: " << c / 2. << std::endl;
		}
	}

	void
	twolevel_reloff_bucketing::store(const std::size_t& idx, const std::size_t& type, const std::size_t& v)
	{
		bucket_info_[idx][type] = guarded_cast<uint8_t>(v);
	}

	std::size_t
	twolevel_reloff_bucketing::load(const std::size_t& idx, const std::size_t& type) const
	{
		return bucket_info_[idx][type];
	}

	void
	twolevel_reloff_bucketing::store_delta(const std::size_t& idx, const std::size_t& type, const std::size_t& id, const long long int& v)
	{
		static const std::size_t mask = 0xff;
		long long int biased_v = v + guarded_cast<long long int>(mask / 2);
		if (biased_v < 0 || biased_v >= guarded_cast<long long int>(mask) - 1)
		{
			// overflow
			//std::cout << "overflow: id: " << id << " v: " << v << " biased_v: " << biased_v << std::endl;
			assert(current_i_ < 1000 || overflow_.size() < current_i_ * 2 / 5);	// allow up to 20 %

			std::pair<overflow_table::iterator, bool> ret = overflow_.insert(std::make_pair(id, guarded_cast<overflow_table::mapped_type>(v)));
			assert(ret.second);	// no duplicate entry
			store(idx, type, mask);
		}
		else
		{
			std::size_t stored_biased_v = guarded_cast<std::size_t>(biased_v);
			store(idx, type, stored_biased_v);
		}
	}

	long long int
	twolevel_reloff_bucketing::load_delta(const std::size_t& idx, const std::size_t& type, const std::size_t& id) const
	{
		static const std::size_t mask = 0xff;
		std::size_t loaded_biased_v = load(idx, type);
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
	twolevel_reloff_bucketing::insert(const std::size_t& index_offset, const std::size_t& dest_offset)
	{
		assert(current_i_ < size_);

		//std::cout << "i: " << current_i_ << std::endl;
		//std::cout << "current: " << index_offset << " " << dest_offset << std::endl;

		std::size_t base_idx = current_i_ / group_size_ * (4 + (group_size_ - 1));
		if (current_i_ % group_size_ == 0)
		{
			store(base_idx + 0, 0, (index_offset & 0xff000000) >> 24);
			store(base_idx + 1, 0, (index_offset & 0x00ff0000) >> 16);
			store(base_idx + 2, 0, (index_offset & 0x0000ff00) >> 8);
			store(base_idx + 3, 0, (index_offset & 0x000000ff) >> 0);

			store(base_idx + 0, 1, (dest_offset & 0xff000000) >> 24);
			store(base_idx + 1, 1, (dest_offset & 0x00ff0000) >> 16);
			store(base_idx + 2, 1, (dest_offset & 0x0000ff00) >> 8);
			store(base_idx + 3, 1, (dest_offset & 0x000000ff) >> 0);
		}
		else
		{
			std::size_t diff_base = current_i_ - 1;
			long long int index_offset_delta = guarded_cast<long long int>(index_offset - last_index_offsets_[diff_base % group_size_]) - guarded_cast<long long int>(bits_per_bucket_);
			long long int dest_offset_delta = guarded_cast<long long int>(dest_offset - last_dest_offsets_[diff_base % group_size_]) - guarded_cast<long long int>(keys_per_bucket_);

#ifdef USE_KEY_DIVERGENCE_FOR_INDEX
			index_offset_delta -= static_cast<long long int>(static_cast<double>(dest_offset_delta) * bits_per_key_);
#endif

			store_delta(base_idx + 4 + current_i_ % group_size_ - 1, 0, current_i_ * 2 + 0, index_offset_delta);
			store_delta(base_idx + 4 + current_i_ % group_size_ - 1, 1, current_i_ * 2 + 1, dest_offset_delta);
		}

		assert(this->index_offset(current_i_) == index_offset);
		assert(this->dest_offset(current_i_) == dest_offset);

		last_index_offsets_[current_i_ % group_size_] = index_offset;
		last_dest_offsets_[current_i_ % group_size_] = dest_offset;

		current_i_++;
	}

	void
	twolevel_reloff_bucketing::finalize()
	{
		std::cout << "overflow: " << static_cast<double>(overflow_.size()) / static_cast<double>(current_i_ * 2) << std::endl;
	}

	std::size_t
	twolevel_reloff_bucketing::index_offset(std::size_t i) const
	{
		assert(i < size_);

		std::size_t base_idx = i / group_size_ * (4 + (group_size_ - 1));
		std::size_t v;

		v = load(base_idx + 0, 0) << 24;
		v |= load(base_idx + 1, 0) << 16;
		v |= load(base_idx + 2, 0) << 8;
		v |= load(base_idx + 3, 0) << 0;

		const std::size_t delta_idx_end = i % group_size_;
		if (delta_idx_end != 0)
		{
			std::size_t load_idx = base_idx + 4;
			std::size_t load_id = (i / group_size_ * group_size_ + 1) * 2 + 0;

			for (std::size_t delta_idx = 1; delta_idx <= delta_idx_end; delta_idx++)
			{
				v += load_delta(load_idx, 0, load_id);

#ifdef USE_KEY_DIVERGENCE_FOR_INDEX
				long long int dest_offset_delta = load_delta(load_idx, 1, load_id + 1);
				v += static_cast<std::size_t>(static_cast<long long int>(static_cast<double>(dest_offset_delta) * bits_per_key_));
#endif

				load_idx++;
				load_id += 2;
			}

			v += (i % group_size_) * bits_per_bucket_;
		}

		//std::cout << "index_offset(" << i << ") = " << v << std::endl;
		return v;
	}

	std::size_t
	twolevel_reloff_bucketing::dest_offset(std::size_t i) const
	{
		assert(i < size_);

		std::size_t base_idx = i / group_size_ * (4 + (group_size_ - 1));
		std::size_t v;

		v = load(base_idx + 0, 1) << 24;
		v |= load(base_idx + 1, 1) << 16;
		v |= load(base_idx + 2, 1) << 8;
		v |= load(base_idx + 3, 1) << 0;

		const std::size_t delta_idx_end = i % group_size_;
		if (delta_idx_end != 0)
		{
			std::size_t load_idx = base_idx + 4;
			std::size_t load_id = (i / group_size_ * group_size_ + 1) * 2 + 1;

			for (std::size_t delta_idx = 1; delta_idx <= delta_idx_end; delta_idx++)
			{
				v += load_delta(load_idx, 1, load_id);

				load_idx++;
				load_id += 2;
			}

			v += (i % group_size_) * keys_per_bucket_;
		}

		//std::cout << "dest_offset(" << i << ") = " << v << std::endl;
		return v;
	}

	std::size_t
	twolevel_reloff_bucketing::size() const
	{
		return size_;
	}

	std::size_t
	twolevel_reloff_bucketing::bit_size() const
	{
		std::size_t bit_size = bucket_info_.size() * 2 * sizeof(uint8_t) * 8;
		// TODO: this assumes a non-chaining hash table with 50% utilization,
		// which is not very true for most std::tr1::unordered_map implementations 
		bit_size += (sizeof(overflow_table::key_type) + sizeof(overflow_table::mapped_type)) * 8 * overflow_.size() * 2;
		return bit_size;
	}
}

