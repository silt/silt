#include "bucketing_index.hpp"
#include <iostream>

// for template instantiation
#include "flat_absoff_bucketing.hpp"
#include "twolevel_absoff_bucketing.hpp"
#include "twolevel_reloff_bucketing.hpp"
#include "semi_direct_16_absoff_bucketing.hpp"
#include "semi_direct_16_reloff_bucketing.hpp"

namespace cindex
{
	template<typename BucketingType>
	bucketing_index<BucketingType>::bucketing_index(std::size_t key_len, std::size_t n, std::size_t bucket_size, std::size_t dest_base, std::size_t dest_keys_per_block, std::size_t skip_bits)
		: key_len_(key_len), n_(n), dest_base_(dest_base), dest_keys_per_block_(dest_keys_per_block), skip_bits_(skip_bits)
	{
		bucket_bits_ = 0;
		while ((guarded_cast<std::size_t>(1) << bucket_bits_) < (n_ / bucket_size))
			bucket_bits_++;
		bucket_count_ = guarded_cast<std::size_t>(1) << bucket_bits_;
		bucket_size_ = n_ / bucket_count_;

		bucketing_.resize(bucket_count_ + 1, bucket_size_, dest_keys_per_block_);
		bucketing_.insert(0, 0);

		last_dest_offset_ = 0;
		pending_bucket_ = 0;
		/*
		pending_keys_ = new key_array_type(key_len, bucket_size * 2 + 256);
		assert(pending_keys_);
		*/
		pending_key_count_ = 0;
	}

	template<typename BucketingType>
	bucketing_index<BucketingType>::bucketing_index(int fd, off_t offset)
	{
		ssize_t len = load_from_file(fd, offset);
		(void)len;
	}

	template<typename BucketingType>
	bucketing_index<BucketingType>::~bucketing_index()
	{
		if (!finalized())
			flush();
	}

	template<typename BucketingType>
	bool
	bucketing_index<BucketingType>::finalized() const
	{
		return pending_bucket_ == bucket_count_;
	}

	template<typename BucketingType>
	bool
	bucketing_index<BucketingType>::insert(const uint8_t* key)
	{
		assert(!finalized());

		std::size_t bucket = find_bucket(key);

		// key order and duplicity checking
		//assert(bucket >= pending_bucket_);
		//assert(pending_key_count_ == 0 || memcmp((*pending_keys_)[pending_key_count_ - 1], key, key_len_) < 0);

		if (pending_bucket_ > bucket ||
			(pending_key_count_ != 0 && memcmp(pending_keys_[pending_key_count_ - 1], key, key_len_) >= 0))
		{
			std::cerr << "cannot insert a non-sorted key" << std::endl;
			return false;
		}

		while (pending_bucket_ < bucket)
			index_pending_keys();

		//assert(pending_key_count_ < pending_keys_->size());
		//memcpy((*pending_keys_)[pending_key_count_++], key, key_len_);
		uint8_t* key_c = new uint8_t[key_len_];
		memcpy(key_c, key, key_len_);
		pending_keys_.push_back(key_c);
		pending_key_count_++;
		return true;
	}

	template<typename BucketingType>
	void
	bucketing_index<BucketingType>::flush()
	{
		assert(!finalized());

		while (pending_bucket_ < bucket_count_)
			index_pending_keys();

		/*
		delete pending_keys_;
		pending_keys_ = NULL;
		*/

		if (finalized())
			bucketing_.finalize();
	}

	struct bucketing_index_state
	{
		std::size_t key_len_;
		std::size_t n_;

		std::size_t bucket_size_;

		std::size_t dest_base_;
		std::size_t dest_keys_per_block_;
		std::size_t skip_bits_;

		// could be calculated but included for simplicity
		std::size_t bucket_count_;
		std::size_t bucket_bits_;
	};

	template<typename BucketingType>
	ssize_t bucketing_index<BucketingType>::load_from_file(int fd, off_t offset)
	{
		bucketing_index_state state;
		ssize_t read_len = pread(fd, &state, sizeof(state), offset);
		assert(read_len == sizeof(state));

		key_len_ = state.key_len_;
		n_ = state.n_;
		bucket_size_ = state.bucket_size_;
		dest_base_ = state.dest_base_;
		dest_keys_per_block_ = state.dest_keys_per_block_;
		skip_bits_ = state.skip_bits_;
		bucket_count_ = state.bucket_count_;
		bucket_bits_ = state.bucket_bits_;

		// TODO: read bucketing information
		// TODO: read trie information
		return 0;
	}

	template<typename BucketingType>
	ssize_t bucketing_index<BucketingType>::store_to_file(int fd, off_t offset)
	{
		assert(finalized());

		bucketing_index_state state;

		state.key_len_ = key_len_;
		state.n_ = n_;
		state.bucket_size_ = bucket_size_;
		state.dest_base_ = dest_base_;
		state.dest_keys_per_block_ = dest_keys_per_block_;
		state.skip_bits_ = skip_bits_;
		state.bucket_count_ = bucket_count_;
		state.bucket_bits_ = bucket_bits_;

		ssize_t wrote_len = pwrite(fd, &state, sizeof(state), offset);
		assert(wrote_len == sizeof(state));

		// TODO: write bucketing information
		// TODO: write trie information
		return 0;
	}

	template<typename BucketingType>
	std::size_t
	bucketing_index<BucketingType>::locate(const uint8_t* key) const
	{
		assert(finalized());

        std::size_t bucket = find_bucket(key);

        std::size_t iter = bucketing_.index_offset(bucket);	// no alignment
        //std::size_t iter = bucketing_.index_offset(bucket) * 4;	// 4-bit aligned
        std::size_t off = bucketing_.dest_offset(bucket);
        std::size_t n = bucketing_.dest_offset(bucket + 1) - off;

        std::size_t key_index = off + trie_.locate(
                repr_, iter,
                key, key_len_,
                0, n,
                dest_base_ + off, dest_keys_per_block_,
                skip_bits_ + bucket_bits_
            );

        //printf("key_index = %lu\n", key_index);
        return key_index;
	}

	template<typename BucketingType>
	std::size_t
	bucketing_index<BucketingType>::find_bucket(const uint8_t* key) const
    {
        std::size_t bits = 0;
        std::size_t index = 0;
        while (bits < bucket_bits_)
		{
            index <<= 1;
            if (skip_bits_ + bits > key_len_ * 8)
			{
				// too short key
				assert(false);
                break;
            }
            if (bit_access::get(key, skip_bits_ + bits))
                index |= 1;
            bits++;
        }

        return index;
    }

	template<typename BucketingType>
	void
	bucketing_index<BucketingType>::index_pending_keys()
	{
		assert(pending_bucket_ < bucket_count_);

		trie_.encode(
				repr_,
				//*pending_keys_, key_len_,
				pending_keys_, key_len_,
				0, pending_key_count_,
				dest_base_ + last_dest_offset_, dest_keys_per_block_,
				skip_bits_ + bucket_bits_
			);

		pending_bucket_ += 1;

		// no alignment
		bucketing_.insert(repr_.size(), last_dest_offset_ + pending_key_count_);

		// add padding for 4-bit alignment
		//std::size_t padding = (4 - (repr_.size() & 3)) & 3;
		//while (padding--)
		//	repr_.push_back(0);
		//assert((repr_.size() & 3) == 0);
		//bucketing_.insert(repr_.size() / 4, last_dest_offset_ + pending_key_count_);

		last_dest_offset_ += pending_key_count_;

		for (std::size_t i = 0; i < pending_key_count_; i++)
			delete [] pending_keys_[i];
		pending_keys_.clear();
		pending_key_count_ = 0;
	}

	template<typename BucketingType>
	std::size_t
	bucketing_index<BucketingType>::bit_size_trie_only() const
	{
		assert(finalized());
		return repr_.size();
	}

	template<typename BucketingType>
	std::size_t
	bucketing_index<BucketingType>::bit_size() const
	{
		assert(finalized());
		return bit_size_trie_only() + bucketing_.bit_size();
	}

	template class bucketing_index<flat_absoff_bucketing<> >;
	template class bucketing_index<twolevel_absoff_bucketing<> >;
	template class bucketing_index<twolevel_reloff_bucketing>;
	template class bucketing_index<semi_direct_16_absoff_bucketing>;
	template class bucketing_index<semi_direct_16_reloff_bucketing>;
}

