/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "fawnds_factory.h"
#include "config.h"
#include <string.h>
#include <unistd.h>
#include <gtest/gtest.h>
#include "print.h"
#include "hashutil.h"

namespace fawn {

    class FawnDSTest : public testing::Test {
    protected:
        // Code here will be called immediately after the constructor (right before
        // each test).
        virtual void SetUp() {
            //h = FawnDS_Factory::Create_FawnDS("./testConfigs/testFawnDS.xml"); // test_num_records_
            h = FawnDS_Factory::New("./testConfigs/testFawnDS.xml"); // test_num_records_
            h->Create();
            //h2 = FawnDS_Factory::Create_FawnDS("/localfs/fawn_db2", num_records_);
        }

        // Code in the TearDown() method will be called immediately after each test
        // (right before the destructor).
        virtual void TearDown() {
            //EXPECT_EQ(0, unlink("/localfs/fawn_db"));
            delete h;
        }

        // Objects declared here can be used by all tests in the test case for HashDB.
        FawnDS *h;
        FawnDS *h2;


        // private:
        // static const uint64_t test_num_records_ = 5000000;
    };

    void test_insert_get(uint32_t order[], uint32_t num, FawnDS *h) {
        char key[10];
        char value[4];
        for (uint32_t i = 0; i < num; i++) {
            memcpy(key, &(order[0]), 4);
            memcpy(key+4, &i, 4);
            memcpy(value, &i, 4);
            ASSERT_TRUE(h->Put(ConstRefValue(key, 8), ConstRefValue(value, 4)) == OK);
        }
        for (uint32_t i = 0; i < num; i++) {
            memcpy(key, &(order[0]), 4);
            memcpy(key+4, &i, 4);
            memcpy(value, &i, 4);
            SizedValue<64> read_data;
            ASSERT_TRUE(h->Get(ConstRefValue(key, 8), read_data) == OK);
            EXPECT_EQ(4, read_data.size());
            EXPECT_STREQ(value, read_data.data());
        }
    }

    // For the following tests to be a hash table tests, you have to comment any usage of hash functions
    // inside the hash table implementation. You also need to set the hash table size to 100.
    TEST_F(FawnDSTest, TestCollisionsA) {
        uint32_t order[10] = { 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000 };
        test_insert_get(order, 10, h);
    }
    TEST_F(FawnDSTest, TestCollisionsB) {
        uint32_t order[10] = { 100, 100, 100, 500, 500, 500, 100, 100, 200, 200 };
        test_insert_get(order, 10, h);
    }
    TEST_F(FawnDSTest, TestCollisionsC) {
        uint32_t order[10] = { 500, 500, 500, 100, 100, 500, 200, 600, 200, 600 };
        test_insert_get(order, 10, h);
    }

    TEST_F(FawnDSTest, TestSimpleInsertRetrieve) {
        const char* key = "key0";
        const char* data = "value0";
        ASSERT_TRUE(h->Put(ConstRefValue(key, strlen(key)), ConstRefValue(data, 7)) == OK);

        SizedValue<64> read_data;
        ASSERT_TRUE(h->Get(ConstRefValue(key, strlen(key)), read_data) == OK);
        EXPECT_EQ(7, read_data.size());
        EXPECT_STREQ(data, read_data.data());
    }


    TEST_F(FawnDSTest, TestSimpleInsertNovalueRetrieve) {
        const char* key = "key0";
        const char* data = "";
        ASSERT_TRUE(h->Put(ConstRefValue(key, strlen(key)), ConstRefValue(data, 0)) == OK);

        SizedValue<64> read_data;
        ASSERT_TRUE(h->Get(ConstRefValue(key, strlen(key)), read_data) == OK);
        EXPECT_EQ(0, read_data.size());
        //EXPECT_STREQ(data, read_data.data());
    }

    TEST_F(FawnDSTest, TestSimpleDelete) {
        //Simple
        const char* key = "key0";
        const char* data = "value0";
        //Delete should return false

        ASSERT_FALSE(h->Delete(ConstRefValue(key, strlen(key))) == OK);
        ASSERT_TRUE(h->Put(ConstRefValue(key, strlen(key)), ConstRefValue(data, 7)) == OK);

        SizedValue<64> read_data;
        ASSERT_TRUE(h->Get(ConstRefValue(key, strlen(key)), read_data) == OK);
        EXPECT_EQ(7, read_data.size());
        EXPECT_STREQ(data, read_data.data());
        // Then delete
        ASSERT_TRUE(h->Delete(ConstRefValue(key, strlen(key))) == OK);
        // Retreive should return false
        ASSERT_FALSE(h->Get(ConstRefValue(key, strlen(key)), read_data) == OK);

        const char* key2 = "key1";
        const char* data2 = "value1";
        ASSERT_TRUE(h->Put(ConstRefValue(key2, strlen(key2)), ConstRefValue(data2, 7)) == OK);
        ASSERT_TRUE(h->Get(ConstRefValue(key2, strlen(key2)), read_data) == OK);
        EXPECT_EQ(7, read_data.size());
        EXPECT_STREQ(data2, read_data.data());
    }

    TEST_F(FawnDSTest, TestDelete) {

        char data[52];
        int* datai = (int*)data;

        for (int i = 0; i < 4; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));

            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Put(ConstRefValue(key), ConstRefValue(data, 52)) == OK);
        }

        int i = 0;
        string key = HashUtil::MD5Hash((char*)&i, sizeof(i));

        ASSERT_TRUE(h->Delete(ConstRefValue(key)) == OK);

        // Spot open
        i = 3;
        key = HashUtil::MD5Hash((char*)&i, sizeof(i));

        for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
            datai[j] = i;
        }
        data[51] = 0;
        ASSERT_TRUE(h->Put(ConstRefValue(key), ConstRefValue(data, 52)) == OK);

        // Spot filled by i=3
        key = HashUtil::MD5Hash((char*)&i, sizeof(i));
        ASSERT_TRUE(h->Delete(ConstRefValue(key)) == OK);

        for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
            datai[j] = i;
        }
        data[51] = 0;

        SizedValue<64> read_data;
        ASSERT_FALSE(h->Get(ConstRefValue(key), read_data) == OK);
    }


    TEST_F(FawnDSTest, Test10000InsertRetrieve) {
        char data[52];

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));

            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Put(ConstRefValue(key), ConstRefValue(data, 52)) == OK);
        }

        for (int i = 0; i < 10000; ++i) {
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;

            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));

            SizedValue<64> read_data;
            ASSERT_TRUE(h->Get(ConstRefValue(key), read_data) == OK);
            EXPECT_EQ(52, read_data.size());
            EXPECT_STREQ(data, read_data.data());
        }
    }

    TEST_F(FawnDSTest, Test10000InsertDelete) {
        char data[52];

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));

            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Put(ConstRefValue(key), ConstRefValue(data, 52)) == OK);
        }

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            //fprintf(stderr, "%d\n", i);
            ASSERT_TRUE(h->Delete(ConstRefValue(key)) == OK);
        }
    }

    TEST_F(FawnDSTest, Test10000InsertRewriteRetrieve) {
        char data[52];

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Put(ConstRefValue(key), ConstRefValue(data, 52)) == OK);
        }

        // FawnDS* h_new = h->Rewrite(NULL);
//         ASSERT_TRUE(h_new != NULL);
//         ASSERT_TRUE(h_new->RenameAndClean(h));
//         h = h_new;

//         for (int i = 0; i < 10000; ++i) {
//             string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
//             string data_ret;
//             int* datai = (int*)data;
//             for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
//                 datai[j] = i;
//             }
//             data[51] = 0;
//             ASSERT_TRUE(h->Get(key.data(), key.length(), data_ret));
//             EXPECT_EQ(52, data_ret.length());
//             EXPECT_STREQ(data, data_ret.data());
//         }

    }


    // fawndb should be the same size as previous fawndbs
    TEST_F(FawnDSTest, Test10000DoubleInsertRewriteRetrieve) {
        char data[52];

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Put(ConstRefValue(key), ConstRefValue(data, 52)) == OK);
        }

        for (int i = 0; i < 10000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Put(ConstRefValue(key), ConstRefValue(data, 52)) == OK);
        }

//         FawnDS<FawnDS_Flash>* h_new = h->Rewrite(NULL);
//         ASSERT_TRUE(h_new != NULL);
//         ASSERT_TRUE(h_new->RenameAndClean(h));
//         h = h_new;

//         for (int i = 0; i < 10000; ++i) {
//             string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
//             string data_ret;
//             int* datai = (int*)data;
//             for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
//                 datai[j] = i;
//             }
//             data[51] = 0;
//             ASSERT_TRUE(h->Get(key.data(), key.length(), data_ret));
//             EXPECT_EQ(52, data_ret.length());
//             EXPECT_STREQ(data, data_ret.data());
//         }

    }

    TEST_F(FawnDSTest, TestWriteDB) {
        char data[1024];

        // 1GB of values
        for (int i = 0; i < 1048576; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            int* datai = (int*)data;
            for (uint j = 0; j < 1024 * sizeof(char) / sizeof(int); ++j) {
              datai[j] = i;
            }
            data[1023] = 0;
            //fprintf(stderr, "%d\n", i);
            ASSERT_TRUE(h->Put(ConstRefValue(key), ConstRefValue(data, 1024)) == OK);
        }

		{
			Value status;
			if (h->Status(MEMORY_USE, status) == OK)
				fprintf(stderr, "Memory use: %s\n", status.str().c_str());
		}

//         // this is required since we're not splitting/merging/rewriting initially
//         ASSERT_TRUE(h->WriteHashtableToFile());

//         // Temporary test of resizing hashtable
//         // with current parameters it does not technically resize.
//         // this was tested with numentries-deletedentries*5 in resizing calculation.
//         FawnDS<FawnDS_Flash>* h_new = h->Rewrite(NULL);
//         ASSERT_TRUE(h_new != NULL);
//         ASSERT_TRUE(h_new->RenameAndClean(h));
//         h = h_new;

    }

    TEST_F(FawnDSTest, Test10000Merge) {
        char data[52];
        for (int i = 0; i < 5000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Put(ConstRefValue(key), ConstRefValue(data, 52)) == OK);
        }

//         for (int i = 5000; i < 10000; ++i) {
//             string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
//             int* datai = (int*)data;
//             for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
//                 datai[j] = i;
//             }
//             data[51] = 0;
//             ASSERT_TRUE(h2->Insert(key.data(), key.length(), data, 52));
//         }


//         ASSERT_TRUE(h2->Merge(h, NULL));

//         for (int i = 0; i < 10000; ++i) {
//             string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
//             string data_ret;
//             ASSERT_TRUE(h2->Get(key.data(), key.length(), data_ret));
//         }

    }

    TEST_F(FawnDSTest, Test1000000Insert) {
        char data[1000];

        for (int i = 0; i < 1000000; ++i) {
            string key = HashUtil::MD5Hash((char*)&i, sizeof(i));
            int* datai = (int*)data;
            for (uint j = 0; j < 52 * sizeof(char) / sizeof(int); ++j) {
                datai[j] = i;
            }
            data[51] = 0;
            ASSERT_TRUE(h->Put(ConstRefValue(key), ConstRefValue(data, 52)) == OK);
        }
   }


}  // namespace fawn

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
