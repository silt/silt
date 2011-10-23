#pragma once

#include "common.hpp"
#include "bit_vector.hpp"
#include "key_array.hpp"
#include "trie.hpp"
#include <vector>

namespace cindex
{
	template<typename BucketingType>
	class bucketing_index
	{
	public:
		typedef BucketingType bucketing_type;

	public:
		bucketing_index(std::size_t key_len, std::size_t n, std::size_t bucket_size, std::size_t dest_base = 0, std::size_t dest_keys_per_block = 1, std::size_t skip_bits = 0);
		bucketing_index(int fd, off_t offset);
		~bucketing_index();

		bool finalized() const CINDEX_WARN_UNUSED_RESULT;

		bool insert(const uint8_t* key);
		void flush();

		ssize_t store_to_file(int fd, off_t offset) CINDEX_WARN_UNUSED_RESULT;

		std::size_t locate(const uint8_t* key) const CINDEX_WARN_UNUSED_RESULT;

		std::size_t bit_size_trie_only() const CINDEX_WARN_UNUSED_RESULT;
		std::size_t bit_size() const CINDEX_WARN_UNUSED_RESULT;

	protected:
		std::size_t find_bucket(const uint8_t* key) const CINDEX_WARN_UNUSED_RESULT;
		void index_pending_keys();

		ssize_t load_from_file(int fd, off_t offset) CINDEX_WARN_UNUSED_RESULT;

		typedef bit_vector<> vector_type;
		typedef key_array key_array_type;

	private:
		std::size_t key_len_;
		std::size_t n_;

		std::size_t bucket_size_;
		
		std::size_t dest_base_;
		std::size_t dest_keys_per_block_;
		std::size_t skip_bits_;

		std::size_t bucket_count_;
		std::size_t bucket_bits_;

        vector_type repr_;
		trie<> trie_;

		bucketing_type bucketing_;

		std::size_t last_dest_offset_;
		std::size_t pending_bucket_;
		//key_array_type* pending_keys_;
		std::vector<const uint8_t*> pending_keys_;
		std::size_t pending_key_count_;
	};
}

