#include "fawnds_factory.h"

#include <cstdlib>
#include <cassert>
#include <vector>
#include <cstdio>
#include <sys/time.h>

struct kv_pair {
	fawn::Value key;
	fawn::Value data;
};
typedef std::vector<kv_pair> kv_array_type;

static void
generate_random_kv(kv_array_type& out_arr, size_t key_len, size_t data_len, size_t size, unsigned int seed = 0)
{
	size_t kv_len = key_len + data_len;

    srand(seed);
    char* buf = new char[kv_len * size];
    for (size_t i = 0; i < kv_len * size; i++)
        buf[i] = rand() & 0xff;
        //buf[i] = (rand() % ('Z' - 'A')) + 'A';

    out_arr.clear();
    for (size_t i = 0; i < size; i++) {
		kv_pair kv;
		kv.key = fawn::RefValue(buf + i * kv_len, key_len);
		kv.data = fawn::RefValue(buf + i * kv_len + key_len, data_len);
        out_arr.push_back(kv);
	}
}

static void
free_kv(kv_array_type& arr)
{
    char* min = reinterpret_cast<char*>(-1);
    for (size_t i = 0; i < arr.size(); i++)
        if (min > arr[i].key.data())
            min = arr[i].key.data();
    delete [] min;
    arr.clear();
}

static void
swap(kv_array_type& arr, size_t i, size_t j)
{
    kv_pair t = arr[i];
    arr[i] = arr[j];
    arr[j] = t;
}

static int 
cmp(kv_array_type& arr, size_t key_len, size_t i, size_t j)
{
    return memcmp(arr[i].key.data(), arr[j].key.data(), key_len);
}

static void
quick_sort(kv_array_type& arr, size_t key_len, size_t left, size_t right)
{
    if (left < right) 
    {
        size_t p = left;
        size_t i = left;
        size_t j = right + 1;
        while (i < j) {
            while (i < right && cmp(arr, key_len, ++i, p) <= 0);
            while (left < j && cmp(arr, key_len, p, --j) <= 0);
            if (i < j)
                swap(arr, i, j);
        }
        swap(arr, p, j);

        if (j > 0)
            quick_sort(arr, key_len, left, j - 1);
        quick_sort(arr, key_len, j + 1, right);
     }
}

static void
sort_keys(kv_array_type& arr, size_t key_len, size_t off, size_t n)
{
    quick_sort(arr, key_len, off, off + n - 1);
}

/*
static void
print_hex(const char* s, int len)
{
    for (int i = 0; i < len; i++)
        printf("%02x", (unsigned char)s[i]);
}
*/

using namespace fawn;

static std::string conf_file = "testConfigs/exp_benchCuckoo.xml";

void benchmark(std::string name)
{
	Configuration* config = new Configuration(conf_file);

	size_t key_len = atoi(config->GetStringValue("child::key-len").c_str());
	size_t size = atoi(config->GetStringValue("child::size").c_str());
	assert(key_len != 0);
	assert(size != 0);

	config->SetContextNode(name);

	kv_array_type arr;
	generate_random_kv(arr, key_len, 0, size);
	sort_keys(arr, key_len, 0, size);

	kv_array_type arr2;
	generate_random_kv(arr2, key_len, 0, size, 1);

	struct timeval tv;
	uint64_t start_time, end_time;

    std::vector<FawnDS*> indexes;

    size_t index_size = 0;

	{
		uint64_t insert_count = 0;
		gettimeofday(&tv, NULL);
		start_time = tv.tv_sec * 1000000L + tv.tv_usec;

		for (size_t i = 0; i < 1024; i++) {
            FawnDS* fawnds = FawnDS_Factory::New(new Configuration(config));
            fawnds->Create();

			size_t j;
			for (j = 0; j < size; j++) {
				if (fawnds->Put(arr[j].key, RefValue(&j)) != OK)
					break;
			}
            index_size = j;
			insert_count += j;

            indexes.push_back(fawnds);
		}

		gettimeofday(&tv, NULL);
		end_time = tv.tv_sec * 1000000L + tv.tv_usec;

		printf("%s: %lf inserts/sec\n", name.c_str(), (double)insert_count / (double)(end_time - start_time) * 1000000L);
		printf("%s: %zu keys per index\n", name.c_str(), index_size);
	}

    for (size_t lookup_mode = 0; lookup_mode < 2; lookup_mode++)
	{
		uint64_t lookup_count = 0;
		gettimeofday(&tv, NULL);
		start_time = tv.tv_sec * 1000000L + tv.tv_usec;

        lookup_count = 10000000;
		for (size_t k = 0; k < lookup_count; k++)
        {
            size_t i = rand() % indexes.size();
            size_t j = rand() % index_size;
            if (lookup_mode == 0)
            {
                FawnDS_ConstIterator it = indexes[i]->Find(arr[j].key);
                while (!it.IsEnd()) {
                    if (it->data.as_number<size_t>() == j)
                        break;
                    ++it;
                }
            }
            else
            {
                FawnDS_ConstIterator it = indexes[i]->Find(arr2[j].key);
                while (!it.IsEnd()) {
                    ++it;
                }
            }
		}

		gettimeofday(&tv, NULL);
		end_time = tv.tv_sec * 1000000L + tv.tv_usec;

		printf("%s: %lf lookups/sec (%s)\n", name.c_str(), (double)lookup_count / (double)(end_time - start_time) * 1000000L, lookup_mode == 0 ? "hit" : "miss");
	}

    for (size_t i = 0; i < indexes.size(); i++)
        delete indexes[i];
    indexes.clear();

	free_kv(arr);
	free_kv(arr2);
	delete config;
}


int main(int argc, char** argv) {
	benchmark("cuckoo");
	return 0;
}

