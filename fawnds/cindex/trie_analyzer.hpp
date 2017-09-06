#pragma once

#include "common.hpp"
#include "bit_access.hpp"
#include <map>
//#include <iostream>

namespace cindex
{
	class trie_analyzer
	{
	public:
		typedef std::map<std::size_t, std::map<std::size_t, std::size_t> > dist_type;

		template<typename KeyArrayType>
		void
		analyze(dist_type& out_dist, const KeyArrayType& arr, std::size_t key_len, std::size_t off, std::size_t n, std::size_t depth = 0) const
		{
			analyze_rec(out_dist, arr, key_len, off, n, depth);
		}

	protected:
		template<typename KeyArrayType>
		void
		analyze_rec(dist_type& out_dist, const KeyArrayType& arr, std::size_t key_len, std::size_t off, std::size_t n, std::size_t depth) const
		{
			//std::cout << "analyze: " << off << " " << n << " " << depth << std::endl;

			//if (n == 0)
			if (n <= 1)
				return;

			if (depth >= key_len * 8)
				return;

			// find the number of keys on the left tree
			std::size_t left = 0;
			for (; left < n; left++)
			{
				if (bit_access::get(arr[off + left], depth))   // assume sorted keys
					break;
			}

			out_dist[n][left]++;

			analyze_rec(out_dist, arr, key_len, off, left, depth + 1);
			analyze_rec(out_dist, arr, key_len, off + left, n - left, depth + 1);
		}
	};
}

