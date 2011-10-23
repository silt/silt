#include "bit_access.hpp"

namespace cindex
{
	/*
#define CINDEX_SHIFT(base)	\
	1ull << ((base) + 7), 1ull << ((base) + 6), 1ull << ((base) + 5), 1ull << ((base) + 4), 1ull << ((base) + 3), 1ull << ((base) + 2), 1ull << ((base) + 1), 1ull << ((base) + 0),

	template<>
	const uint8_t bit_table<uint8_t>::bit_table_[block_info<uint8_t>::bits_per_block] =
	{
		CINDEX_SHIFT(0)
	};

	template<>
	const uint16_t bit_table<uint16_t>::bit_table_[block_info<uint16_t>::bits_per_block] =
	{
		CINDEX_SHIFT(8)
		CINDEX_SHIFT(0)
	};

	template<>
	const uint32_t bit_table<uint32_t>::bit_table_[block_info<uint32_t>::bits_per_block] =
	{
		CINDEX_SHIFT(24)
		CINDEX_SHIFT(16)
		CINDEX_SHIFT(8)
		CINDEX_SHIFT(0)
	};

	template<>
	const uint64_t bit_table<uint64_t>::bit_table_[block_info<uint64_t>::bits_per_block] =
	{
		CINDEX_SHIFT(56)
		CINDEX_SHIFT(48)
		CINDEX_SHIFT(40)
		CINDEX_SHIFT(32)
		CINDEX_SHIFT(24)
		CINDEX_SHIFT(16)
		CINDEX_SHIFT(8)
		CINDEX_SHIFT(0)
	};

#undef CINDEX_SHIFT
	*/
}

