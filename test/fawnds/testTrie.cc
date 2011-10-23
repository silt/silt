#include "fawnds_factory.h"

#include <gtest/gtest.h>
#include <cstdlib>
#include <cassert>

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

namespace fawn
{
	static std::string conf_file = "testConfigs/testTrie.xml";

    class FawnDS_SF_Ordered_Trie_Test : public testing::Test
    {
    protected:
        // Code here will be called immediately after the constructor (right before
        // each test).
        virtual void SetUp() {
			Configuration* config = new Configuration(conf_file);
			key_len_ = atoi(config->GetStringValue("child::key-len").c_str());
			data_len_ = atoi(config->GetStringValue("child::data-len").c_str());
			size_ = atoi(config->GetStringValue("child::size").c_str());
			assert(key_len_ != 0);
			assert(data_len_ != 0);
			assert(size_ != 0);

            generate_random_kv(arr_, key_len_, data_len_, size_);

            fawnds_ = FawnDS_Factory::New(config);
			fawnds_->Create();

			ret_data_.resize(0);
        }

        // Code in the TearDown() method will be called immediately after each test
        // (right before the destructor).
        virtual void TearDown() {
            free_kv(arr_);

            delete fawnds_;
        }

        // Objects declared here can be used by all tests in the test case for HashDB.

        size_t key_len_;
        size_t data_len_;
        size_t size_;

        kv_array_type arr_;

        FawnDS* fawnds_;

		Value ret_data_;
    };

    TEST_F(FawnDS_SF_Ordered_Trie_Test, TestSimpleInsertRetrieve1) {
        EXPECT_EQ(OK, fawnds_->Put(arr_[0].key, arr_[0].data));
        EXPECT_EQ(OK, fawnds_->Flush());

        EXPECT_EQ(OK, fawnds_->Get(arr_[0].key, ret_data_));
        EXPECT_EQ(data_len_, ret_data_.size());
        EXPECT_EQ(0, memcmp(arr_[0].data.data(), ret_data_.data(), data_len_));
    }

    TEST_F(FawnDS_SF_Ordered_Trie_Test, TestSimpleInsertRetrieve2) {
        arr_[0].key.data()[0] = 5;
        arr_[1].key.data()[0] = 7;

        EXPECT_EQ(OK, fawnds_->Put(arr_[0].key, arr_[0].data));
        EXPECT_EQ(OK, fawnds_->Put(arr_[1].key, arr_[1].data));
        EXPECT_EQ(OK, fawnds_->Flush());

        EXPECT_EQ(OK, fawnds_->Get(arr_[0].key, ret_data_));
        EXPECT_EQ(data_len_, ret_data_.size());
        EXPECT_EQ(0, memcmp(arr_[0].data.data(), ret_data_.data(), data_len_));

        EXPECT_EQ(OK, fawnds_->Get(arr_[1].key, ret_data_));
        EXPECT_EQ(data_len_, ret_data_.size());
        EXPECT_EQ(0, memcmp(arr_[1].data.data(), ret_data_.data(), data_len_));
    }

    TEST_F(FawnDS_SF_Ordered_Trie_Test, TestSimpleUnsortedInsertRetrieve4) {
        arr_[0].key.data()[0] = 5;
        arr_[1].key.data()[0] = 7;
        arr_[2].key.data()[0] = 3;
        arr_[3].key.data()[0] = 9;

        EXPECT_EQ(OK, fawnds_->Put(arr_[0].key, arr_[0].data));
        EXPECT_EQ(OK, fawnds_->Put(arr_[1].key, arr_[1].data));
        EXPECT_EQ(INVALID_KEY, fawnds_->Put(arr_[2].key, arr_[2].data));
        EXPECT_EQ(OK, fawnds_->Put(arr_[3].key, arr_[3].data));
        EXPECT_EQ(OK, fawnds_->Flush());

        EXPECT_EQ(OK, fawnds_->Get(arr_[0].key, ret_data_));
        EXPECT_EQ(data_len_, ret_data_.size());
        EXPECT_EQ(0, memcmp(arr_[0].data.data(), ret_data_.data(), data_len_));

        EXPECT_EQ(OK, fawnds_->Get(arr_[1].key, ret_data_));
        EXPECT_EQ(data_len_, ret_data_.size());
        EXPECT_EQ(0, memcmp(arr_[1].data.data(), ret_data_.data(), data_len_));

        EXPECT_EQ(KEY_NOT_FOUND, fawnds_->Get(arr_[2].key, ret_data_));

        EXPECT_EQ(OK, fawnds_->Get(arr_[3].key, ret_data_));
        EXPECT_EQ(data_len_, ret_data_.size());
        EXPECT_EQ(0, memcmp(arr_[3].data.data(), ret_data_.data(), data_len_));
    }

    TEST_F(FawnDS_SF_Ordered_Trie_Test, TestSimpleSortedInsertRetrieve4) {
        arr_[0].key.data()[0] = 3;
        arr_[1].key.data()[0] = 5;
        arr_[2].key.data()[0] = 7;
        arr_[3].key.data()[0] = 9;

        EXPECT_EQ(OK, fawnds_->Put(arr_[0].key, arr_[0].data));
        EXPECT_EQ(OK, fawnds_->Put(arr_[1].key, arr_[1].data));
        EXPECT_EQ(OK, fawnds_->Put(arr_[2].key, arr_[2].data));
        EXPECT_EQ(OK, fawnds_->Put(arr_[3].key, arr_[3].data));
        EXPECT_EQ(OK, fawnds_->Flush());

        EXPECT_EQ(OK, fawnds_->Get(arr_[0].key, ret_data_));
        EXPECT_EQ(data_len_, ret_data_.size());
        EXPECT_EQ(0, memcmp(arr_[0].data.data(), ret_data_.data(), data_len_));

        EXPECT_EQ(OK, fawnds_->Get(arr_[1].key, ret_data_));
        EXPECT_EQ(data_len_, ret_data_.size());
        EXPECT_EQ(0, memcmp(arr_[1].data.data(), ret_data_.data(), data_len_));

        EXPECT_EQ(OK, fawnds_->Get(arr_[2].key, ret_data_));
        EXPECT_EQ(data_len_, ret_data_.size());
        EXPECT_EQ(0, memcmp(arr_[2].data.data(), ret_data_.data(), data_len_));

        EXPECT_EQ(OK, fawnds_->Get(arr_[3].key, ret_data_));
        EXPECT_EQ(data_len_, ret_data_.size());
        EXPECT_EQ(0, memcmp(arr_[3].data.data(), ret_data_.data(), data_len_));
    }

    TEST_F(FawnDS_SF_Ordered_Trie_Test, TestSimpleSortedInsertRetrieveSome) {
        sort_keys(arr_, key_len_, 0, 100);
 
        for (size_t i = 0; i < 100; i++)
            EXPECT_EQ(OK, fawnds_->Put(arr_[i].key, arr_[i].data));
        EXPECT_EQ(OK, fawnds_->Flush());

        for (size_t i = 0; i < 100; i++)
        {
            EXPECT_EQ(OK, fawnds_->Get(arr_[i].key, ret_data_));
            EXPECT_EQ(data_len_, ret_data_.size());
            EXPECT_EQ(0, memcmp(arr_[i].data.data(), ret_data_.data(), data_len_));
        }
    }

    TEST_F(FawnDS_SF_Ordered_Trie_Test, TestSimpleSortedInsertRetrieveMany) {
        sort_keys(arr_, key_len_, 0, size_);
 
        for (size_t i = 0; i < size_; i++)
            EXPECT_EQ(OK, fawnds_->Put(arr_[i].key, arr_[i].data));
        EXPECT_EQ(OK, fawnds_->Flush());

		{
			Value status;
			if (fawnds_->Status(MEMORY_USE, status) == OK)
				fprintf(stderr, "Memory use: %s\n", status.str().c_str());
		}

        for (size_t i = 0; i < size_; i++)
        {
            EXPECT_EQ(OK, fawnds_->Get(arr_[i].key, ret_data_));
            EXPECT_EQ(data_len_, ret_data_.size());
            EXPECT_EQ(0, memcmp(arr_[i].data.data(), ret_data_.data(), data_len_));
        }
    }

//    TEST_F(FawnDS_SF_Ordered_Trie_Test, TestSimpleSortedInsertRetrieveManyReloaded) {
//        sort_keys(arr_, key_len_, 0, size_);
// 
//        for (size_t i = 0; i < size_; i++)
//            EXPECT_EQ(0, fawnds_->Insert(arr_[i], key_len_, arr_[i] + key_len_, data_len_));
//        EXPECT_EQ(0, fawnds_->Flush());
//
//        delete fawnds_;
//        fawnds_ = FawnDS_Factory::Open_FawnDS(conf_file);
//
//        for (size_t i = 0; i < size_; i++)
//        {
//            EXPECT_EQ(0, fawnds_->Get(arr_[i], key_len_, ret_data_, ret_data_len_));
//            EXPECT_EQ(data_len_, ret_data_len_);
//            EXPECT_EQ(0, memcmp(arr_[i] + key_len_, ret_data_, ret_data_len_));
//        }
//    }
//
    TEST_F(FawnDS_SF_Ordered_Trie_Test, TestSimpleIterator) {
        sort_keys(arr_, key_len_, 0, size_);
 
        for (size_t i = 0; i < size_; i++)
            EXPECT_EQ(OK, fawnds_->Put(arr_[i].key, arr_[i].data));
        EXPECT_EQ(OK, fawnds_->Flush());

        size_t idx = 0;
		for (FawnDS_ConstIterator it = fawnds_->Enumerate(); !it.IsEnd(); ++it)
		{
			EXPECT_EQ(key_len_, it->key.size());
			EXPECT_EQ(data_len_, it->data.size());
			if (idx < size_) {
				EXPECT_EQ(0, memcmp(arr_[idx].key.data(), it->key.data(), key_len_));
				EXPECT_EQ(0, memcmp(arr_[idx].data.data(), it->data.data(), data_len_));
			}
			idx++;
		}
        EXPECT_EQ(size_, idx);
    }

    TEST_F(FawnDS_SF_Ordered_Trie_Test, TestGetBeforeFinalizing) {
        sort_keys(arr_, key_len_, 0, size_);
 
        EXPECT_EQ(OK, fawnds_->Put(arr_[0].key, arr_[0].data));
        EXPECT_EQ(ERROR, fawnds_->Get(arr_[0].key, ret_data_));
    }

    TEST_F(FawnDS_SF_Ordered_Trie_Test, TestInsertAfterFinalizing) {
        sort_keys(arr_, key_len_, 0, size_);
 
        EXPECT_EQ(OK, fawnds_->Flush());
        EXPECT_EQ(ERROR, fawnds_->Put(arr_[0].key, arr_[0].data));
    }

    TEST_F(FawnDS_SF_Ordered_Trie_Test, TestFlushAfterFinalizing) {
        sort_keys(arr_, key_len_, 0, size_);
 
        EXPECT_EQ(OK, fawnds_->Flush());
        EXPECT_EQ(OK, fawnds_->Flush());
    }

//    TEST_F(FawnDS_SF_Ordered_Trie_Test, TestInsertAfterReloading) {
//        sort_keys(arr_, key_len_, 0, size_);
// 
//        EXPECT_EQ(0, fawnds_->Insert(arr_[0], key_len_, arr_[0] + key_len_, data_len_));
//        EXPECT_EQ(0, fawnds_->Flush());
//
//        delete fawnds_;
//        fawnds_ = FawnDS_Factory::Open_FawnDS(conf_file);
//
//        EXPECT_EQ(-1, fawnds_->Insert(arr_[1], key_len_, arr_[1] + key_len_, data_len_));
//    }
//
//    TEST_F(FawnDS_SF_Ordered_Trie_Test, TestFlushAfterReloading) {
//        sort_keys(arr_, key_len_, 0, size_);
// 
//        EXPECT_EQ(0, fawnds_->Insert(arr_[0], key_len_, arr_[0] + key_len_, data_len_));
//        EXPECT_EQ(0, fawnds_->Flush());
//
//        delete fawnds_;
//        fawnds_ = FawnDS_Factory::Open_FawnDS(conf_file);
//
//        EXPECT_EQ(-1, fawnds_->Flush());
//    }

}  // namespace fawn

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

