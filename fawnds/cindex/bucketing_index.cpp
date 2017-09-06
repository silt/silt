#include "bucketing_index.hpp"

// for template instantiation
#include "flat_absoff_bucketing.hpp"
#include "twolevel_absoff_bucketing.hpp"
#include "twolevel_reloff_bucketing.hpp"
#include "semi_direct_16_absoff_bucketing.hpp"
#include "semi_direct_16_reloff_bucketing.hpp"

namespace cindex
{
	template<typename BucketingType, typename TrieType>
	bucketing_index<BucketingType, TrieType>::bucketing_index()
		: initialized_(false)
	{
	}

	template<typename BucketingType, typename TrieType>
	bucketing_index<BucketingType, TrieType>::bucketing_index(std::size_t key_len, std::size_t n, std::size_t bucket_size, std::size_t dest_base, std::size_t dest_keys_per_block, std::size_t skip_bits, trie_analyzer::dist_type* in_dist, trie_analyzer::dist_type* out_dist)
		: initialized_(true), key_len_(key_len), n_(n), dest_base_(dest_base), dest_keys_per_block_(dest_keys_per_block), skip_bits_(skip_bits), in_dist_(in_dist), out_dist_(out_dist)
	{
		if (n_ == 0)
		{
			// avoid division by zero in several places
			n = n_ = 1;
		}

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

		if (in_dist_)
			trie_.recreate_huffman_from_dist(*in_dist_);
	}

	template<typename BucketingType, typename TrieType>
	void
	bucketing_index<BucketingType, TrieType>::serialize(serializer& s) const
	{
		assert(initialized_);
		assert(finalized());

		s << key_len_;
		s << n_;
		s << bucket_size_;
		s << dest_base_;
		s << dest_keys_per_block_;
		s << skip_bits_;
		s << bucket_count_;
		s << bucket_bits_;

		s << repr_;
		s << bucketing_;
	}

	template<typename BucketingType, typename TrieType>
	void
	bucketing_index<BucketingType, TrieType>::deserialize(serializer& s)
	{
		assert(!initialized_);

		s >> key_len_;
		s >> n_;
		s >> bucket_size_;
		s >> dest_base_;
		s >> dest_keys_per_block_;
		s >> skip_bits_;
		s >> bucket_count_;
		s >> bucket_bits_;

		s >> repr_;
		s >> bucketing_;

		in_dist_ = NULL;
		out_dist_ = NULL;

		last_dest_offset_ = 0;
		pending_bucket_ = bucket_count_;	// assume finalized state
		pending_key_count_ = 0;

		initialized_ = true;

	}

	template<typename BucketingType, typename TrieType>
	bucketing_index<BucketingType, TrieType>::~bucketing_index()
	{
		if (initialized_)
		{
			if (!finalized())
				flush();
		}
	}

	template<typename BucketingType, typename TrieType>
	bool
	bucketing_index<BucketingType, TrieType>::finalized() const
	{
		assert(initialized_);
		return pending_bucket_ == bucket_count_;
	}

	template<typename BucketingType, typename TrieType>
	bool
	bucketing_index<BucketingType, TrieType>::insert(const uint8_t* key)
	{
		assert(initialized_);
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

	template<typename BucketingType, typename TrieType>
	void
	bucketing_index<BucketingType, TrieType>::flush()
	{
		assert(initialized_);
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

	template<typename BucketingType, typename TrieType>
	std::size_t
	bucketing_index<BucketingType, TrieType>::locate(const uint8_t* key) const
	{
		assert(initialized_);
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

	template<typename BucketingType, typename TrieType>
	std::size_t
	bucketing_index<BucketingType, TrieType>::find_bucket(const uint8_t* key) const
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

	template<typename BucketingType, typename TrieType>
	void
	bucketing_index<BucketingType, TrieType>::index_pending_keys()
	{
		assert(pending_bucket_ < bucket_count_);

		if (!out_dist_)
		{
			trie_.encode(
					repr_,
					//*pending_keys_, key_len_,
					pending_keys_, key_len_,
					0, pending_key_count_,
					dest_base_ + last_dest_offset_, dest_keys_per_block_,
					skip_bits_ + bucket_bits_
				);
		}
		else
		{
			trie_analyzer().analyze(
					*out_dist_,
					//*pending_keys_, key_len_,
					pending_keys_, key_len_,
					0, pending_key_count_,
					skip_bits_ + bucket_bits_
				);
		}

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

	template<typename BucketingType, typename TrieType>
	std::size_t
	bucketing_index<BucketingType, TrieType>::bit_size_trie_only() const
	{
		assert(initialized_);
		assert(finalized());
		return repr_.size();
	}

	template<typename BucketingType, typename TrieType>
	std::size_t
	bucketing_index<BucketingType, TrieType>::bit_size() const
	{
		assert(initialized_);
		assert(finalized());
		return bit_size_trie_only() + bucketing_.bit_size();
	}

	template class bucketing_index<flat_absoff_bucketing<> >;
	template class bucketing_index<twolevel_absoff_bucketing<> >;
	template class bucketing_index<twolevel_absoff_bucketing<>, trie<true, 64> >;
	template class bucketing_index<twolevel_reloff_bucketing>;
	template class bucketing_index<semi_direct_16_absoff_bucketing>;
	template class bucketing_index<semi_direct_16_reloff_bucketing>;
}

