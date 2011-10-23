#pragma once

#include "common.hpp"

namespace cindex
{
	class expected_size
	{
	public:
		static double index_size(std::size_t keys_per_bucket, std::size_t keys_per_block);
	};
}

