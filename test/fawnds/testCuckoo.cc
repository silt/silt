#include <vector>
#include "fawnds_factory.h"

#include "gtest/gtest.h"

#define MAXNUM  262144L
using namespace std;

namespace fawn
{

	static std::string conf_file = "testConfigs/testCuckoo.xml";
    uint64_t key_vec1[MAXNUM];
    uint64_t val_vec1[MAXNUM];

    class FawnDS_Cuckoo_Test : public testing::Test
    {
    public:
        FawnDS *h;


    protected:
        virtual void SetUp() {
            // Code here will be called immediately after the constructor (right before
            // each test).
            srand ( time(NULL) );

            h = FawnDS_Factory::New(conf_file); // test_num_records_
            FawnDS_Return r = h->Create();
			assert(r == OK);

            uint32_t tag;
            char* key;
            char* val;
            for (uint32_t i = 0; i < MAXNUM ; i++) {
                key = (char*) &(key_vec1[i]);
                val = (char*) &(val_vec1[i]);
                tag =  ((rand() % 65535) << 16) + (rand() % 65535);
                memcpy(key, &i, 4);
                memcpy(key+4, &tag, 4);
                val_vec1[i] = rand() % 65535;
                //memcpy(val, &i, 4);
            }
            
        }

        virtual void TearDown() {
            // Code here will be called immediately after each test (right
            // before the destructor).
			delete h;
        }

    };

    void test_put_get(uint32_t num, FawnDS *&h, bool stop_if_full = false) {
        char *key;
        char *val;
		uint32_t actual_num = num;
        for (uint32_t i = 0; i < num; i++) {
            key = (char*) &(key_vec1[i]);
            val = (char*) &(val_vec1[i]);
			FawnDS_Return ret = h->Put(ConstRefValue(key, 8), ConstRefValue(val, 4));
			if (stop_if_full) {
				if (ret == INSUFFICIENT_SPACE) {
					actual_num = i;
					break;
				}
			}
			else
				ASSERT_TRUE(ret == OK);
        }

        // should see all keys already inserted 
        for (uint32_t i = 0; i < actual_num; i++) {
            key = (char*) &(key_vec1[i]);
            val = (char*) &(val_vec1[i]);
            SizedValue<64> read_data;
            ASSERT_TRUE(h->Get(ConstRefValue(key, 8), read_data) == OK);
            EXPECT_EQ((unsigned) 4, read_data.size());
            EXPECT_EQ(strncmp(val, read_data.data(), 4), 0);
        }

        // should not see the key failed in inserting
        key = (char*) &(key_vec1[actual_num]);
        val = (char*) &(val_vec1[actual_num]);
        SizedValue<64> read_data;
        ASSERT_TRUE(h->Get(ConstRefValue(key, 8), read_data) == KEY_NOT_FOUND);

        h->Close();
    }

    void test_put_undo_get(FawnDS *&h) {
        char *key;
        char *val;
        long i = 0;
        FawnDS_Return r;
        for (i = 0; i < MAXNUM; i++) {
            key = (char*) &(key_vec1[i]);
            val = (char*) &(val_vec1[i]);
            r = h->Put(ConstRefValue(key, 8), ConstRefValue(val, 4));
            if (r != OK)
                break;
        };
        ASSERT_TRUE ( r == INSUFFICIENT_SPACE);
        long actual_num = i;
        for (i = 0; i < actual_num; i++) {
            key = (char*) &(key_vec1[i]);
            val = (char*) &(val_vec1[i]);
            SizedValue<64> read_data;
            ASSERT_TRUE(h->Get(ConstRefValue(key, 8), read_data) == OK);
            EXPECT_EQ((unsigned) 4, read_data.size());
            EXPECT_EQ(strncmp(val, read_data.data(), 4), 0);
        }
        h->Close();
    }

    void test_loadfactor(FawnDS *&h) {
        char *key;
        char *val;
        long count = 0;
        long i = 0;
        int round = 1;
        float average = 0;
        int MAXROUND = 10;
        while (round <= MAXROUND) {
            key = (char*) &(key_vec1[i]);
            val = (char*) &(val_vec1[i]);
            FawnDS_Return r = h->Put(ConstRefValue(key, 8), ConstRefValue(val, 4));
            if (r == OK) 
                count ++;
            else{
                Value table_size;
                h->Status(CAPACITY, table_size);
                uint32_t  hash_table_size = atoi(table_size.str().c_str());
                printf("trial %d, %ld keys inserted, loadfactor = %.2f%%\n", round, count, 100.0* count/ hash_table_size);
                average +=  100.0* count/ hash_table_size;
                h->Close();
                delete h;
                h = FawnDS_Factory::New(conf_file); // test_num_records_
				assert(h);
                r = h->Create();
				assert(r == OK);
                count = 0;
                round ++;
            }
            i ++;
            i = i % MAXNUM;
        }
        printf("average load factor =%.2f%%\n", average / MAXROUND);
        h->Close();
    }

    TEST_F(FawnDS_Cuckoo_Test, SimpleTest_PutGet1Key) {
        test_put_get(1, h);
    }

    TEST_F(FawnDS_Cuckoo_Test, SimpleTest_PutGet1000Keys) {
        test_put_get(1000, h);
    }

    TEST_F(FawnDS_Cuckoo_Test, SimpleTest_PutGetManyKeys) {
        test_put_get(1000000, h, true);
    }

    TEST_F(FawnDS_Cuckoo_Test, SimpleTest_PutUndoGet) {
        test_put_undo_get(h);
    }

    TEST_F(FawnDS_Cuckoo_Test, SimpleTest_Loadfactor) {
        test_loadfactor(h);
    }


}  // namespace

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
