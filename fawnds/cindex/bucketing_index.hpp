#pragma once

#include "common.hpp"
#include "bit_vector.hpp"
#include "key_array.hpp"
#include "trie.hpp"
#include "trie_analyzer.hpp"
#include "serializer.hpp"
#include <vector>

namespace cindex
{
	template<typename BucketingType, typename TrieType = trie<> >
	class bucketing_index
	{
	public:
		typedef BucketingType bucketing_type;

	public:
		bucketing_index();

		bucketing_index(std::size_t key_len, std::size_t n, std::size_t bucket_size, std::size_t dest_base = 0, std::size_t dest_keys_per_block = 1, std::size_t skip_bits = 0, trie_analyzer::dist_type* in_dist = NULL, trie_analyzer::dist_type* out_dist = NULL);
		bucketing_index(int fd, off_t& offset);
		~bucketing_index();

		void serialize(serializer& s) const;
		void deserialize(serializer& s);

		bool finalized() const CINDEX_WARN_UNUSED_RESULT;

		bool insert(const uint8_t* key);
		void flush();

		std::size_t locate(const uint8_t* key) const CINDEX_WARN_UNUSED_RESULT;

		std::size_t bit_size_trie_only() const CINDEX_WARN_UNUSED_RESULT;
		std::size_t bit_size() const CINDEX_WARN_UNUSED_RESULT;

	protected:
		std::size_t find_bucket(const uint8_t* key) const CINDEX_WARN_UNUSED_RESULT;
		void index_pending_keys();

		typedef bit_vector<> vector_type;
		typedef key_array key_array_type;

	private:
		bool initialized_;

		std::size_t key_len_;
		std::size_t n_;

		std::size_t bucket_size_;
		
		std::size_t dest_base_;
		std::size_t dest_keys_per_block_;
		std::size_t skip_bits_;

		trie_analyzer::dist_type* in_dist_;
		trie_analyzer::dist_type* out_dist_;
		
		std::size_t bucket_count_;
		std::size_t bucket_bits_;

        vector_type repr_;
		TrieType trie_;

		bucketing_type bucketing_;

		std::size_t last_dest_offset_;
		std::size_t pending_bucket_;
		//key_array_type* pending_keys_;
		std::vector<const uint8_t*> pending_keys_;
		std::size_t pending_key_count_;
	};
}

