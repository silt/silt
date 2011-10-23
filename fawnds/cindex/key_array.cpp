#include "key_array.hpp"
#include <cstring>

namespace cindex
{
	key_array::key_array(std::size_t key_len, std::size_t size)
		: key_len_(key_len), size_(size), v_(new uint8_t[key_len * size])
	{
		assert(v_);
	}

	key_array::~key_array()
	{
		delete [] v_;
		v_ = NULL;
	}

	const uint8_t*
	key_array::operator[](std::size_t i) const
	{
		assert(i < size_);
		return v_ + i * key_len_;
	}

	uint8_t*
	key_array::operator[](std::size_t i)
	{
		assert(i < size_);
		return v_ + i * key_len_;
	}

	std::size_t
	key_array::size() const
	{
		return size_;
	}

	void
	key_array::generate_random_keys(std::size_t off, std::size_t n, unsigned int seed)
	{
		assert(off + n <= size_);

		uint8_t key[key_len_];

		srand(seed);
		for (std::size_t i = off; i < off + n; i++)
		{
			for (std::size_t j = 0; j < key_len_; j++)
				key[j] = static_cast<uint8_t>(rand());
			memcpy((*this)[i], key, key_len_);
		}
	}
}

