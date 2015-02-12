#include "trie.hpp"

#include <cstdio>
#include <unistd.h>

namespace cindex
{
	uint64_t trie_stats::time_encode_total = 0;
	uint64_t trie_stats::time_encode_golomb = 0;
	uint64_t trie_stats::time_encode_huffman = 0;
	uint64_t trie_stats::time_decode_total = 0;
	uint64_t trie_stats::time_decode_golomb = 0;
	uint64_t trie_stats::time_decode_huffman = 0;
	void trie_stats::print()
	{
#ifdef BENCHMARK
		uint64_t onesec = 1;
		{
			scoped_stopwatch sw(onesec);
			sleep(1);
		}
		printf("time_encode_total:   %lf ms\n", (double)time_encode_total / (double)onesec * 1000.);
		printf("time_encode_golomb:  %lf ms\n", (double)time_encode_golomb / (double)onesec * 1000.);
		printf("time_encode_huffman: %lf ms\n", (double)time_encode_huffman / (double)onesec * 1000.);
		printf("time_decode_total:   %lf ms\n", (double)time_decode_total / (double)onesec * 1000.);
		printf("time_decode_golomb:  %lf ms\n", (double)time_decode_golomb / (double)onesec * 1000.);
		printf("time_decode_huffman: %lf ms\n", (double)time_decode_huffman / (double)onesec * 1000.);
#endif
	}
}
