#include "bit_vector.hpp"
#include "key_array.hpp"
#include "stopwatch.hpp"

#include "flat_absoff_bucketing.hpp"
#include "twolevel_absoff_bucketing.hpp"
#include "twolevel_reloff_bucketing.hpp"
#include "semi_direct_16_absoff_bucketing.hpp"
#include "semi_direct_16_reloff_bucketing.hpp"

#include "bucketing_index.hpp"

//#include "bitmap_index.hpp"

#include "sorting.hpp"

#include <cstdio>

using namespace cindex;

typedef bit_vector<> buffer_type; 

template<typename IndexType>
static
void
bench()
{
	//std::size_t kv_len = 32;
	std::size_t kv_len = 20;
	std::size_t key_len = 20;
	//std::size_t size = 4 * 1048576;	// 4 Mi entries; 128 MiB
	std::size_t size = 123406;	// for SOSP paper

	//std::size_t kv_per_block_list[] = {1, 4, 16};
	std::size_t kv_per_block_list[] = {1};

	stopwatch ss;

	// initialize input data
	key_array arr(kv_len, size);
	arr.generate_random_keys(0, size);
	quick_sort::sort(arr, 0, size);

	key_array arr2(kv_len, size);
	arr2.generate_random_keys(0, size, 1);

	for (std::size_t i = 1; i < size; i++)
		assert(memcmp(arr[i - 1], arr[i], key_len) < 0);

	for (std::size_t kv_per_block_i = 0; kv_per_block_i < sizeof(kv_per_block_list) / sizeof(kv_per_block_list[0]); kv_per_block_i++)
	{
		std::size_t kv_per_block = kv_per_block_list[kv_per_block_i];

		//for (std::size_t group_size = 1; group_size <= 1024; group_size *= 2)
		//for (std::size_t group_size = 128; group_size <= 1024; group_size *= 2)
		for (std::size_t group_size = 256; group_size <= 256; group_size *= 2)
		{
            std::vector<IndexType*> indexes;

            // construct
            ss.start();
            for (size_t i = 0; i < 1024; i++)
            {
                IndexType* s = new IndexType(key_len, size, group_size, 0, kv_per_block);

                for (std::size_t kv_i = 0; kv_i < size; kv_i++)
                    s->insert(arr[kv_i]);
                s->flush();

                indexes.push_back(s);
            }
            ss.stop();
            uint64_t const_time = ss.real_time();

            // lookup
            //std::size_t lookups = 10000000 / group_size;
            const std::size_t lookups = 10000000;	// for SOSP paper
            uint64_t lookup_time_hit = 1;
            uint64_t lookup_time_miss = 1;
            for (std::size_t lookup_mode = 0; lookup_mode < 2; lookup_mode++)
            {
                srand(0);
                ss.start();
                for (std::size_t lookup_i = 0; lookup_i < lookups; lookup_i++)
                {
                    std::size_t i = static_cast<std::size_t>(rand()) % indexes.size();
                    std::size_t kv_i = static_cast<std::size_t>(rand()) % size;
                    if (lookup_mode == 0)
                    {
                        std::size_t idx = indexes[i]->locate(arr[kv_i]);
                        assert(kv_i / kv_per_block == idx / kv_per_block);
                        (void)idx;
                    }
                    else
                    {
                        std::size_t idx = indexes[i]->locate(arr2[kv_i]);
                        (void)idx;
                    }
                }
                ss.stop();
                if (lookup_mode == 0)
                    lookup_time_hit = ss.real_time();
                else
                    lookup_time_miss = ss.real_time();
            }

            printf("kv_per_block: %lu\n", kv_per_block);
            printf("group_size: %lu\n", group_size);
            printf("bits_per_key: %lf\n", static_cast<double>(indexes[0]->bit_size()) / static_cast<double>(size));
            printf("bits_per_key_trie_only: %lf\n", static_cast<double>(indexes[0]->bit_size_trie_only()) / static_cast<double>(size));
            printf("const_time_us: %lf\n", static_cast<double>(const_time) / static_cast<double>(indexes.size() * size) / 1000.);
            printf("lookup_time_us_hit: %lf\n", static_cast<double>(lookup_time_hit) / static_cast<double>(lookups) / 1000.);
            printf("lookup_time_us_miss: %lf\n", static_cast<double>(lookup_time_miss) / static_cast<double>(lookups) / 1000.);
            printf("\n");

            for (size_t i = 0; i < indexes.size(); i++)
                delete indexes[i];
            indexes.clear();
		}
	}

	trie_stats::print();
}

int
main(int argc CINDEX_UNUSED, char* argv[] CINDEX_UNUSED)
{
#ifndef NDEBUG
	printf("program is compiled without NDEBUG\n\n"); 
#endif

	//printf("bitmap_index<twolevel_absoff_bucketing<> >\n");
	//bench<bitmap_index<twolevel_absoff_bucketing<> > >();

	//printf("bitmap_index<twolevel_reloff_bucketing>\n");
	//bench<bitmap_index<twolevel_reloff_bucketing> >();

	//printf("bucketing: flat_absoff\n");
	//bench<bucketing_index<flat_absoff_bucketing<> > >();

	printf("bucketing: twolevel_absoff\n");
	bench<bucketing_index<twolevel_absoff_bucketing<> > >();

	//printf("bucketing: twolevel_reloff\n");
	//bench<bucketing_index<twolevel_reloff_bucketing> >();

	//printf("bucketing: semi_direct_16_absoff\n");
	//bench<bucketing_index<semi_direct_16_absoff_bucketing> >();

	//printf("bucketing: semi_direct_16_reloff\n");
	//bench<bucketing_index<semi_direct_16_reloff_bucketing > >();

	return 0;
}

