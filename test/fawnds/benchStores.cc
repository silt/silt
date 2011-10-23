#include "fawnds_factory.h"
#include "print.h"

#include <cstdlib>
#include <cassert>
#include <vector>
#include <cstdio>
#include <sys/time.h>
#include <tbb/atomic.h>
#include <tbb/queuing_mutex.h>

using namespace fawn;

static std::string conf_file = "testConfigs/exp_benchStores.xml";

// TODO: moving these values into configuration file
// assuming 4 partitions (instances)
const int constructor_thread_counts[3] = {4, 4, 4};
// successful lookup needs some threads (empirically about 16 for good performance)
// while unsuccessful lookup (except SortedStore) makes threshing with too many threads
const int reader_thread_counts[3][2] = {{16, 4}, {16, 4}, {16, 16}};


Configuration* g_main_config;

std::vector<FawnDS*> g_log_stores;
std::vector<FawnDS*> g_hash_stores;
std::vector<FawnDS*> g_sorted_stores;

size_t g_key_len;
size_t g_data_len;
size_t g_size;              // the size of each of LogStore and HashStore
size_t g_store_count;       // the number of LogStores or HashStores
size_t g_merge_size;        // how many HashStores will be merged into a SortedStore
size_t g_get_count;         // how many operations to use to measure GET speed
size_t g_phase;             // the current phase (0 = LogStore, 1 = LogStore->HashStore, 2 = some HashStore->SortedStore)
std::string g_temp_file;    // temporary directory for sorter
bool g_successful_lookup;   // should the reader make lookups for existing keys or non-exsistent keys
const size_t get_batch_size = 1000;
size_t g_epoch;              // the epoch of all the test
const size_t g_max_epoch = 2;

tbb::atomic<size_t> g_current_store_i;
tbb::atomic<size_t> g_current_get_i;
tbb::atomic<size_t> g_merged_count;
tbb::atomic<size_t> g_next_id;

void set_config_param(Configuration* main_config, Configuration* store_config)
{
    char buf[1024];

    snprintf(buf, sizeof(buf), "%zu", ++g_next_id - 1);
    store_config->SetStringValue("child::id", buf);

    store_config->SetStringValue("child::key-len", main_config->GetStringValue("child::key-len"));
    store_config->SetStringValue("child::data-len", main_config->GetStringValue("child::data-len"));
    if (store_config->ExistsNode("child::size") == 0) {
        snprintf(buf, sizeof(buf), "%zu", g_size * g_merge_size * (g_epoch + 1));
        store_config->SetStringValue("child::size", buf);
    }
}

inline uint32_t my_rand_r(uint64_t* v)
{
    // Java's
    return (*v = (25214903917ULL * *v + 11ULL) & ((1ULL << 48) - 1)) >> 16;
}

void generate_kv(char* key, char* data, size_t store_id, size_t key_id)
{
    // determistically generate random key-value pairs
    uint64_t seed = store_id * g_size + key_id;

    for (size_t i = 0; i < g_key_len; i += 4) {
        *reinterpret_cast<uint32_t*>(key + i) = my_rand_r(&seed);
    }

    if (data) {
        uint64_t r = (static_cast<uint64_t>(my_rand_r(&seed)) << 32) | my_rand_r(&seed);
        uint64_t* p = reinterpret_cast<uint64_t*>(data);
        const uint64_t* p_end = p + (g_data_len / sizeof(uint64_t));
        while (p != p_end)
            *p++ = r++;
    }
}

void* constructor_thread_main(void* arg)
{
    assert(g_phase < 3);

    char key[(g_key_len + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t)];
    char data[(g_data_len + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t)];

    while (true) {
        size_t current_store_i = ++g_current_store_i - 1;
        if (g_phase == 0 || g_phase == 1) {
            if (current_store_i >= g_store_count)
                break;
        }
        else {
            if (current_store_i >= (g_store_count + g_merge_size - 1) / g_merge_size)
                break;
        }

        //printf("creating store %zu out of %zu\n", current_store_i + 1, g_store_count);

        Configuration* store_config = new Configuration(g_main_config, true);
        if (g_phase == 0)
            store_config->SetContextNode("store0");
        else if (g_phase == 1)
            store_config->SetContextNode("store1");
        else
            store_config->SetContextNode("store2");
        set_config_param(g_main_config, store_config);

        FawnDS* fawnds = FawnDS_Factory::New(store_config);
        assert(fawnds);

        if (fawnds->Create() != OK)
            assert(false);

        if (g_phase == 0) {
            for (size_t j = 0; j < g_size; j++) {
                generate_kv(key, data, current_store_i + g_epoch * g_store_count, j);

                if (fawnds->Put(ConstRefValue(key, g_key_len), ConstRefValue(data, g_data_len)) != OK)
                    assert(false);
            }
            g_log_stores[current_store_i] = fawnds;
        }
        else if (g_phase == 1) {
            if (g_log_stores[current_store_i]->ConvertTo(fawnds) != OK)
                assert(false);
            g_log_stores[current_store_i]->Close();
            g_log_stores[current_store_i]->Destroy();
            delete g_log_stores[current_store_i];
            g_log_stores[current_store_i] = NULL;
            g_hash_stores[current_store_i] = fawnds;
        }
        else {
            // TODO: this code is largely borrowed from FawnDS_Combi;
            // it would be probably useful to make FawnDS more flexible to expose this sorting functionally to the external.
            FawnDS* sorter;
            Configuration* sorter_config = new Configuration();

            char buf[1024];

            if (sorter_config->CreateNodeAndAppend("type", ".") != 0)
                assert(false);
            if (sorter_config->SetStringValue("type", "sorter") != 0)
                assert(false);

            if (sorter_config->CreateNodeAndAppend("key-len", ".") != 0)
                assert(false);
            snprintf(buf, sizeof(buf), "%zu", g_key_len);
            if (sorter_config->SetStringValue("key-len", buf) != 0)
                assert(false);

            if (sorter_config->CreateNodeAndAppend("data-len", ".") != 0)
                assert(false);
            snprintf(buf, sizeof(buf), "%zu", 1 + g_data_len);
            if (sorter_config->SetStringValue("data-len", buf) != 0)
                assert(false);

            if (sorter_config->CreateNodeAndAppend("temp-file", ".") != 0)
                assert(false);
            if (sorter_config->SetStringValue("temp-file", g_temp_file) != 0)
                assert(false);

            sorter = FawnDS_Factory::New(sorter_config);
            if (!sorter)
                assert(false);
            if (sorter->Create() != OK)
                assert(false);

            for (size_t j = 0; j < g_merge_size; j++) {
                if (current_store_i * g_merge_size + j >= g_hash_stores.size())
                    break;
                FawnDS_ConstIterator it = g_hash_stores[current_store_i * g_merge_size + j]->Enumerate();
                while (!it.IsEnd()) {
                    Value combined_data;
                    combined_data.resize(1 + g_data_len);
                    combined_data.data()[0] = 0;    // is delete?
                    memcpy(combined_data.data() + 1, it->data.data(), it->data.size());

                    if (sorter->Put(it->key, combined_data) != OK)
                        assert(false);

                    ++it;
                }
            }

            if (sorter->Flush() != OK)
                assert(false);

            Value key;
            Value data;
            bool deleted;

            FawnDS_ConstIterator it_b = g_sorted_stores[current_store_i]->Enumerate();
            FawnDS_ConstIterator it_m = sorter->Enumerate();
            while (true) {
                bool select_middle_store;
                if (!it_b.IsEnd() && !it_m.IsEnd()) {
                    if (it_b->key < it_m->key) {
                        select_middle_store = false;
                    }
                    else if (it_b->key > it_m->key) {
                        select_middle_store = true;
                    }
                    else
                        assert(false);
                }
                else if (!it_b.IsEnd()) {
                    select_middle_store = false;
                }
                else if (!it_m.IsEnd()) {
                    select_middle_store = true;
                }
                else
                    break;

                if (!select_middle_store) {
                    key = it_b->key;
                    data = ConstRefValue(it_b->data.data() + 1, g_data_len);
                }
                else {
                    key = it_m->key;
                    data = ConstRefValue(it_m->data.data() + 1, g_data_len);
                }
                if (fawnds->Put(key, data) != OK)
                    assert(false);

                g_merged_count++;
                if (!select_middle_store)
                    ++it_b;
                else
                    ++it_m;
            }

            delete sorter;

            for (size_t j = 0; j < g_merge_size; j++) {
                if (current_store_i * g_merge_size + j >= g_hash_stores.size())
                    break;
                g_hash_stores[current_store_i * g_merge_size + j]->Close();
                g_hash_stores[current_store_i * g_merge_size + j]->Destroy();
                delete g_hash_stores[current_store_i * g_merge_size + j];
                g_hash_stores[current_store_i * g_merge_size + j] = NULL;
            }

            g_sorted_stores[current_store_i]->Close();
            g_sorted_stores[current_store_i]->Destroy();
            delete g_sorted_stores[current_store_i];
            g_sorted_stores[current_store_i] = fawnds;
        }

        if (fawnds->Flush() != OK)
            assert(false);
    }
    
    return NULL;
}

void* reader_thread_main(void *arg)
{
    assert(g_phase < 3);

    char key[(g_key_len + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t)];
    Value data;

    static tbb::queuing_mutex mutex;

    while (true) {
        size_t current_get_i = (g_current_get_i += get_batch_size) - get_batch_size;
        if (current_get_i >= g_get_count)
            break;

        uint64_t seed;
        {
            tbb::queuing_mutex::scoped_lock lock(mutex);
            seed = rand();
        }
        seed += current_get_i;

        for (size_t trial = 0; trial < get_batch_size; trial++) {
            size_t i = my_rand_r(&seed) % g_store_count;
            size_t j = my_rand_r(&seed) % g_size;

            if (g_successful_lookup)
                generate_kv(key, NULL, i + g_epoch * g_store_count, j);
            else
                generate_kv(key, NULL, i + g_epoch * g_store_count + g_max_epoch * g_store_count, j);

            FawnDS* fawnds;

            if (g_phase == 0) {
                assert(i < g_log_stores.size());
                fawnds = g_log_stores[i];
            }
            else if (g_phase == 1) {
                assert(i < g_hash_stores.size());
                fawnds = g_hash_stores[i];
            }
            else {
                assert(i / g_merge_size < g_sorted_stores.size());
                fawnds = g_sorted_stores[i / g_merge_size];
            }
            assert(fawnds);

            FawnDS_Return ret = fawnds->Get(ConstRefValue(key, g_key_len), data);
            FawnDS_Return expected_ret;
            if (g_successful_lookup)
                expected_ret = OK;
            else
                expected_ret = KEY_NOT_FOUND;

            if (ret != expected_ret) {
                fprintf(stderr, "unexpected result: %d (expected: %d) #%zu, key:\n", ret, expected_ret, current_get_i);
                print_payload(reinterpret_cast<const unsigned char*>(key), g_key_len, 4);

                char expected_data[(g_data_len + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t)];
                if (g_successful_lookup)
                    generate_kv(key, expected_data, i + g_epoch * g_store_count, j);
                else
                    generate_kv(key, expected_data, i + g_epoch * g_store_count + g_max_epoch * g_store_count, j);

                if (data.size() != g_data_len || memcmp(expected_data, data.data(), g_data_len) != 0) {
                    fprintf(stderr, "  data mismatch: %zu\n", data.size());
                    print_payload(reinterpret_cast<const unsigned char*>(data.data()), data.size(), 4);
                    fprintf(stderr, "  expected: %zu\n", g_data_len);
                    print_payload(reinterpret_cast<const unsigned char*>(expected_data), g_data_len, 4);
                }

                assert(false);
            }
        }
    }

    return NULL;
}

void load_params()
{
	g_main_config = new Configuration(conf_file);

	g_key_len = atoi(g_main_config->GetStringValue("child::key-len").c_str());
	g_data_len = atoi(g_main_config->GetStringValue("child::data-len").c_str());
	g_size = atoi(g_main_config->GetStringValue("child::size").c_str());
	g_store_count = atoi(g_main_config->GetStringValue("child::store-count").c_str());
	g_merge_size = atoi(g_main_config->GetStringValue("child::merge-size").c_str());
	g_get_count = atoi(g_main_config->GetStringValue("child::get-count").c_str());
    g_temp_file = g_main_config->GetStringValue("child::temp-file");
	assert(g_key_len != 0);
	assert(g_data_len != 0);
	assert(g_size != 0);
	assert(g_store_count != 0);
	assert(g_merge_size != 0);
	assert(g_get_count != 0);
    //g_store_count = 64;
    //g_get_count = 100000;

    printf("key-len: %zu\n", g_key_len);
    printf("data-len: %zu\n", g_data_len);
    printf("size: %zu\n", g_size);
    printf("store-count: %zu\n", g_store_count);
    printf("get-count: %zu\n", g_get_count);
    printf("temp-file: %s\n", g_temp_file.c_str());
    printf("\n");
    fflush(stdout);
}

void benchmark()
{
    g_epoch = 0;
    g_next_id = 0;

    g_log_stores.resize(g_store_count, NULL);
    g_hash_stores.resize(g_store_count, NULL);
    g_sorted_stores.resize((g_store_count + g_merge_size - 1) / g_merge_size, NULL);
    for (size_t i = 0; i < g_sorted_stores.size(); i++) {
        Configuration* store_config = new Configuration(g_main_config, true);
        store_config->SetContextNode("store2");
        set_config_param(g_main_config, store_config);

        FawnDS* fawnds = FawnDS_Factory::New(store_config);
        assert(fawnds);

        if (fawnds->Create() != OK)
            assert(false);
        g_sorted_stores[i] = fawnds;
        fawnds->Flush();
    }

    while (g_epoch < g_max_epoch) {
        printf("epoch: %zu\n", g_epoch);
        fflush(stdout);

        sync();

        struct timeval tv;
        uint64_t start_time, end_time;

        g_phase = 0;
        g_merged_count = 0;

        while (g_phase < 3) {
            g_current_store_i = 0;

            {
                gettimeofday(&tv, NULL);
                start_time = tv.tv_sec * 1000000L + tv.tv_usec;

                std::vector<pthread_t> tids;
                for (size_t k = 0; k < constructor_thread_counts[g_phase]; k++) {
                    pthread_t tid;
                    if (pthread_create(&tid, NULL, constructor_thread_main, NULL))
                        perror("");
                    tids.push_back(tid);
                }

                for (size_t k = 0; k < constructor_thread_counts[g_phase]; k++)
                    pthread_join(tids[k], NULL);
                tids.clear();

                sync();

                gettimeofday(&tv, NULL);
                end_time = tv.tv_sec * 1000000L + tv.tv_usec;
            }

            printf("%zu: %lf keys/sec for construction\n", g_phase, (double)(g_store_count * g_size) / (double)(end_time - start_time) * 1000000L);
            fflush(stdout);

            //Value status;
            //if (g_stores[0]->Status(MEMORY_USE, status) != OK)
            //    assert(false);
            //printf("%zu: MEMORY_USE: %s (one store)\n", g_phase, status.str().c_str());
            //fflush(stdout);

            sleep(10);

            sync();

            for (int trial = 0; trial < 2; trial++) {
                for (int lookup_mode = 0; lookup_mode < 2; lookup_mode++) {
                    g_current_get_i = 0;
                    if (lookup_mode == 0)
                        g_successful_lookup = true;
                    else
                        g_successful_lookup = false;

                    gettimeofday(&tv, NULL);
                    start_time = tv.tv_sec * 1000000L + tv.tv_usec;

                    std::vector<pthread_t> tids;
                    for (size_t k = 0; k < reader_thread_counts[g_phase][lookup_mode]; k++) {
                        pthread_t tid;
                        if (pthread_create(&tid, NULL, reader_thread_main, NULL))
                            perror("");
                        tids.push_back(tid);
                    }

                    for (size_t k = 0; k < reader_thread_counts[g_phase][lookup_mode]; k++)
                        pthread_join(tids[k], NULL);
                    tids.clear();

                    gettimeofday(&tv, NULL);
                    end_time = tv.tv_sec * 1000000L + tv.tv_usec;

                    printf("%zu: %lf GETs/sec (%s)\n", g_phase, (double)g_get_count / (double)(end_time - start_time) * 1000000L, lookup_mode == 0 ? "hit" : "miss");
                    fflush(stdout);
                }
            }

            printf("\n");
            fflush(stdout);

            g_phase++;
        }

        printf("\n");
        fflush(stdout);

        assert(g_merged_count == g_store_count * g_size * (g_epoch + 1));
        g_epoch++;
    }

    for (size_t i = 0; i < g_sorted_stores.size(); i++) {
        g_sorted_stores[i]->Close();
        g_sorted_stores[i]->Destroy();
        delete g_sorted_stores[i];
        g_sorted_stores[i] = NULL;
    }

    sync();

    delete g_main_config;
}

int main(int argc, char** argv) {
    load_params();
	benchmark();

	return 0;
}

