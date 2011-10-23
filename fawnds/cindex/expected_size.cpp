#include "expected_size.hpp"
#include <iostream>

namespace cindex
{
	static struct
	{
		std::size_t keys_per_bucket;
		std::size_t keys_per_block;
		bool weak_ranking;
		double bits_per_key;
	} known_index_sizes[] = {
		{1, 	1,	false,	0.000000},
		{2, 	1,	false,	1.500000},
		{4, 	1,	false,	2.035714},
		{8, 	1,	false,	2.368062},
		{16, 	1,	false,	2.565474},
		{32, 	1,	false,	2.719423},
		{64, 	1,	false,	2.800122},
		{128, 	1,	false,	2.846470},
		{256, 	1,	false,	2.872846},
		{512, 	1,	false,	2.887720},
		{1024,	1,	false,	2.896029},

		{1, 	1,	true,	0.000000},
		{2, 	1,	true,	1.000000},
		{4, 	1,	true,	1.595238},
		{8, 	1,	true,	1.925228},
		{16, 	1,	true,	2.122761},
		{32, 	1,	true,	2.276723},
		{64, 	1,	true,	2.357425},
		{128, 	1,	true,	2.403773},
		{256, 	1,	true,	2.430150},
		{512, 	1,	true,	2.445024},
		{1024,	1,	true,	2.453333},

		{1, 	4,	false,	0.000000},
		{2, 	4,	false,	0.375000},
		{4, 	4,	false,	0.750000},
		{8, 	4,	false,	1.153312},
		{16, 	4,	false,	1.355466},
		{32, 	4,	false,	1.509626},
		{64, 	4,	false,	1.590345},
		{128, 	4,	false,	1.636697},
		{256, 	4,	false,	1.663074},
		{512, 	4,	false,	1.677949},
		{1024,	4,	false,	1.686258},

		{1, 	4,	true,	0.000000},
		{2, 	4,	true,	0.250000},
		{4, 	4,	true,	0.651786},
		{8, 	4,	true,	1.048069},
		{16, 	4,	true,	1.249690},
		{32, 	4,	true,	1.403818},
		{64, 	4,	true,	1.484533},
		{128, 	4,	true,	1.530884},
		{256, 	4,	true,	1.557261},
		{512, 	4,	true,	1.572136},
		{1024,	4,	true,	1.580445},

		{1, 	16,	false,	0.000000},
		{2, 	16,	false,	0.093750},
		{4, 	16,	false,	0.130371},
		{8, 	16,	false,	0.202756},
		{16, 	16,	false,	0.382474},
		{32, 	16,	false,	0.530633},
		{64, 	16,	false,	0.609876},
		{128, 	16,	false,	0.655798},
		{256, 	16,	false,	0.682011},
		{512, 	16,	false,	0.696813},
		{1024,	16,	false,	0.705088},

		{1, 	16,	true,	0.000000},
		{2, 	16,	true,	0.062500},
		{4, 	16,	true,	0.119838},
		{8, 	16,	true,	0.197576},
		{16, 	16,	true,	0.377405},
		{32, 	16,	true,	0.525653},
		{64, 	16,	true,	0.604917},
		{128, 	16,	true,	0.650845},
		{256, 	16,	true,	0.677060},
		{512, 	16,	true,	0.691863},
		{1024,	16,	true,	0.700139},
	};

	double
	expected_size::index_size(std::size_t keys_per_bucket CINDEX_UNUSED, std::size_t keys_per_block CINDEX_UNUSED)
	{
		const bool weak_ranking = false;
		for (std::size_t i = 0; i < sizeof(known_index_sizes) / sizeof(known_index_sizes[0]); i++)
		{
			if (known_index_sizes[i].keys_per_bucket == keys_per_bucket &&
				known_index_sizes[i].keys_per_block == keys_per_block &&
				known_index_sizes[i].weak_ranking == weak_ranking)
				return known_index_sizes[i].bits_per_key;
		}
		std::cerr << "bug: no index size information availble for keys_per_bucket = " << keys_per_bucket << ", keys_per_block = " << keys_per_block << std::endl;
		//assert(false);
		return 3.;
	}
}

