#pragma once

#include "common.hpp"

namespace cindex
{
	class key_array
	{
	public:
		key_array(std::size_t key_len, std::size_t size);

		~key_array();

		const uint8_t* operator[](std::size_t i) const CINDEX_WARN_UNUSED_RESULT;

		uint8_t* operator[](std::size_t i) CINDEX_WARN_UNUSED_RESULT;

		std::size_t key_len() const CINDEX_WARN_UNUSED_RESULT { return key_len_; }
		std::size_t size() const CINDEX_WARN_UNUSED_RESULT;

		void generate_random_keys(std::size_t off, std::size_t n, unsigned int seed = 0);

	private:
		std::size_t key_len_;
		std::size_t size_;
		uint8_t* v_;
	};
}

