#pragma once 

#include "common.hpp"
#include <boost/integer/static_log2.hpp>

namespace cindex
{
	template<typename BlockType>
	class block_info
	{
	public:
		typedef BlockType block_type;

		static const std::size_t bytes_per_block = sizeof(block_type);
		static const std::size_t bits_per_block = bytes_per_block * 8;
		static const std::size_t bit_mask = bits_per_block - 1;
		static const std::size_t log_bits_per_block = boost::static_log2<bits_per_block>::value;

		static std::size_t
		block_count(std::size_t n) CINDEX_WARN_UNUSED_RESULT
		{
			return (n + bits_per_block - 1) / bits_per_block;
		}

		static std::size_t
		size(std::size_t n) CINDEX_WARN_UNUSED_RESULT
		{
			return block_count(n) * bytes_per_block;
		}
	};
}

