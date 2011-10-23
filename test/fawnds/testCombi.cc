#include "fawnds_factory.h"
#include "rate_limiter.h"

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
	static std::string conf_file = "testConfigs/testCombi.xml";

    class FawnDS_Combi_Test : public testing::Test
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

            fawnds_ = FawnDS_Factory::New(config);
			fawnds_->Create();

			ret_data_.resize(0);
        }

        // Code in the TearDown() method will be called immediately after each test
        // (right before the destructor).
        virtual void TearDown() {
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

    TEST_F(FawnDS_Combi_Test, TestSimpleInsertRetrieve1) {
		generate_random_kv(arr_, key_len_, data_len_, 1);

        EXPECT_EQ(OK, fawnds_->Put(arr_[0].key, arr_[0].data));

        EXPECT_EQ(OK, fawnds_->Get(arr_[0].key, ret_data_));
        EXPECT_EQ(data_len_, ret_data_.size());
        EXPECT_EQ(0, memcmp(arr_[0].data.data(), ret_data_.data(), data_len_));

		free_kv(arr_);
    }

    TEST_F(FawnDS_Combi_Test, TestSimpleInsertRetrieve2) {
		generate_random_kv(arr_, key_len_, data_len_, 2);

        arr_[0].key.data()[0] = 5;
        arr_[1].key.data()[0] = 7;

        EXPECT_EQ(OK, fawnds_->Put(arr_[0].key, arr_[0].data));
        EXPECT_EQ(OK, fawnds_->Put(arr_[1].key, arr_[1].data));

        EXPECT_EQ(OK, fawnds_->Get(arr_[0].key, ret_data_));
        EXPECT_EQ(data_len_, ret_data_.size());
        EXPECT_EQ(0, memcmp(arr_[0].data.data(), ret_data_.data(), data_len_));

        EXPECT_EQ(OK, fawnds_->Get(arr_[1].key, ret_data_));
        EXPECT_EQ(data_len_, ret_data_.size());
        EXPECT_EQ(0, memcmp(arr_[1].data.data(), ret_data_.data(), data_len_));

		free_kv(arr_);
    }

    TEST_F(FawnDS_Combi_Test, TestSimpleInsertRetrieve4) {
		generate_random_kv(arr_, key_len_, data_len_, 4);

        arr_[0].key.data()[0] = 5;
        arr_[1].key.data()[0] = 7;
        arr_[2].key.data()[0] = 3;
        arr_[3].key.data()[0] = 9;

        EXPECT_EQ(OK, fawnds_->Put(arr_[0].key, arr_[0].data));
        EXPECT_EQ(OK, fawnds_->Put(arr_[1].key, arr_[1].data));
        EXPECT_EQ(OK, fawnds_->Put(arr_[2].key, arr_[2].data));
        EXPECT_EQ(OK, fawnds_->Put(arr_[3].key, arr_[3].data));

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

		free_kv(arr_);
    }

    TEST_F(FawnDS_Combi_Test, TestSimpleSortedInsertRetrieveSome) {
		generate_random_kv(arr_, key_len_, data_len_, 100);

        for (size_t i = 0; i < 100; i++)
            EXPECT_EQ(OK, fawnds_->Put(arr_[i].key, arr_[i].data));

        for (size_t i = 0; i < 100; i++)
        {
            EXPECT_EQ(OK, fawnds_->Get(arr_[i].key, ret_data_));
            EXPECT_EQ(data_len_, ret_data_.size());
            EXPECT_EQ(0, memcmp(arr_[i].data.data(), ret_data_.data(), data_len_));
        }

		free_kv(arr_);
    }

    TEST_F(FawnDS_Combi_Test, TestSimpleSortedInsertRetrieveSomeSome) {
		generate_random_kv(arr_, key_len_, data_len_, 100);

        for (size_t i = 0; i < 100; i++)
            EXPECT_EQ(OK, fawnds_->Put(arr_[i].key, arr_[i].data));

		// update data with the same key
        for (size_t i = 0; i < 100; i++) {
			arr_[i].data.data()[0] = 255 - arr_[i].data.data()[0];
            EXPECT_EQ(OK, fawnds_->Put(arr_[i].key, arr_[i].data));
		}

        for (size_t i = 0; i < 100; i++)
        {
            EXPECT_EQ(OK, fawnds_->Get(arr_[i].key, ret_data_));
            EXPECT_EQ(data_len_, ret_data_.size());
            EXPECT_EQ(0, memcmp(arr_[i].data.data(), ret_data_.data(), data_len_));
        }

		free_kv(arr_);
    }

    TEST_F(FawnDS_Combi_Test, TestSimpleSortedInsertRetrieveMany) {
		generate_random_kv(arr_, key_len_, data_len_, size_);

		int64_t operations_per_sec = 100000L;
		RateLimiter rate_limiter(0, operations_per_sec, operations_per_sec / 1000L, 1000000000L / 1000L);

        for (size_t i = 0; i < size_; i++) {
			rate_limiter.remove_tokens(1);
            EXPECT_EQ(OK, fawnds_->Put(arr_[i].key, arr_[i].data));
		}

		EXPECT_EQ(OK, fawnds_->Flush());
		sleep(5);

        for (size_t i = 0; i < size_; i++)
        {
			rate_limiter.remove_tokens(1);
            EXPECT_EQ(OK, fawnds_->Get(arr_[i].key, ret_data_));
            EXPECT_EQ(data_len_, ret_data_.size());
            EXPECT_EQ(0, memcmp(arr_[i].data.data(), ret_data_.data(), data_len_));
        }

		free_kv(arr_);
    }

    TEST_F(FawnDS_Combi_Test, TestSimpleSortedInsertRetrieveManyMany) {
		generate_random_kv(arr_, key_len_, data_len_, size_);

		int64_t operations_per_sec = 100000L;
		RateLimiter rate_limiter(0, operations_per_sec, operations_per_sec / 1000L, 1000000000L / 1000L);

        for (size_t i = 0; i < size_; i++) {
			rate_limiter.remove_tokens(1);
            EXPECT_EQ(OK, fawnds_->Put(arr_[i].key, arr_[i].data));
		}

        for (size_t i = 0; i < size_; i++) {
			rate_limiter.remove_tokens(1);
			arr_[i].data.data()[0] = 255 - arr_[i].data.data()[0];
            EXPECT_EQ(OK, fawnds_->Put(arr_[i].key, arr_[i].data));
		}

		// don't wait so that background merge and Get() happen simultaneously
		//EXPECT_EQ(OK, fawnds_->Flush());
		//sleep(5);

        for (size_t i = 0; i < size_; i++)
        {
			rate_limiter.remove_tokens(1);
            EXPECT_EQ(OK, fawnds_->Get(arr_[i].key, ret_data_));
            EXPECT_EQ(data_len_, ret_data_.size());
            EXPECT_EQ(0, memcmp(arr_[i].data.data(), ret_data_.data(), data_len_));
        }

		free_kv(arr_);
    }

    TEST_F(FawnDS_Combi_Test, DISABLED_TestSimpleSortedInsertRetrieveManyOnTheFly) {
		int64_t operations_per_sec = 100000L;
		RateLimiter rate_limiter(0, operations_per_sec, operations_per_sec / 1000L, 1000000000L / 1000L);

		srand(0);
        for (size_t i = 0; i < 10000000; i++) {
			//rate_limiter.remove_tokens(1);

			char key[key_len_ + data_len_];
			for (size_t i = 0; i < key_len_ + data_len_; i++)
				key[i] = rand() & 0xff;

            EXPECT_EQ(OK, fawnds_->Put(RefValue(key, key_len_), RefValue(key + key_len_, data_len_)));
		}

		EXPECT_EQ(OK, fawnds_->Flush());
		sleep(5);
	}

    TEST_F(FawnDS_Combi_Test, TestFlush) {
        EXPECT_EQ(OK, fawnds_->Flush());
        EXPECT_EQ(OK, fawnds_->Flush());
    }

}  // namespace fawn

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

