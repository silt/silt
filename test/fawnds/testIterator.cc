/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "fawnds_factory.h"
#include "hashutil.h"
#include "debug.h"

using namespace fawn;
using namespace std;

FawnDS* db;

/*

void CallForEach(const uint32_t &key_len, const uint32_t &data_len, const char* const &key, const char* const &data, const bool &isDeleted)
{
    printf("Test: CallForEach: %s\n", data);
}

void CallForEach2(const uint32_t &key_len, const uint32_t &data_len, const char* const &key, const char* const &data, const bool &isDeleted)
{
    char* gData;
    uint32_t gData_len;
    if (db->Get(key, key_len, gData, gData_len) == 0 && data_len == gData_len && memcmp(data, gData, data_len) == 0) {
        printf("Test: CallForEach2: %s\n", data);
    }
}

void CallBeforeRemain() {}
*/

int main(int argc, char** argv)
{
    printf("Start testIterator!\n");
    int max = 100;
    string config_file = "./testConfigs/testIterator.xml";
    DPRINTF(2, "TestIterator: Will create FawnDS now!\n");
    FawnDS* db = FawnDS_Factory::New(config_file);
    db->Create();

    for(int i = 0; i < max; ++i)
        {
            uint32_t key_id = HashUtil::FNVHash((const void *)&i, 4);
            char key[4];
            memcpy(key, &key_id, 4);
            char data[40];
            sprintf(data, "%i %s", i, "aaaa");
            if (db->Put(RefValue(key, 4), RefValue(data, strlen(data)+1)) != 0) {
                printf("Insert failed!\n");
            exit(-1);
            } else {
                printf("Inserted: %s\n", data);//                printf("Inserted: %s with key: %s\n", data, key);
            }
        }

    for(int i = max/4; i < 3*max/4; ++i)
        {
            uint32_t key_id = HashUtil::FNVHash((const void *)&i, 4);
            char key[4];
            memcpy(key, &key_id, 4);
            char data[40];
            sprintf(data, "%i %s", i, "bbbb");
            if (db->Put(RefValue(key, 4), RefValue(data, strlen(data)+1)) != 0) {
                printf("Insert failed!");
            } else {
                printf("Inserted: %s\n", data);//                 printf("Inserted: %s with key: %s\n", data, key);
            }
        }

    /*
    if(db->DoForEach(CallForEach, CallBeforeRemain) != 0)
        printf("DoForEach failed!");

    if(db->DoForEach(CallForEach2, CallBeforeRemain) != 0)
        printf("DoForEach #2 failed!");
    */

    {
        FawnDS_Iterator it = db->Enumerate();
        while (!it.IsEnd())
        {
            printf("Test: CallForEach: %s\n", it->data.data());
            ++it;
        }

        Value read_data;
        it = db->Enumerate();
        while (!it.IsEnd())
        {
            if (db->Get(it->key, read_data) == OK && it->data == read_data) {
                printf("Test: CallForEach2: %s\n", it->data.data());
            }
            ++it;
        }
    }

    // all iterators need to be destroyed

    /*
    auto_ptr<FawnDS_Iterator> it = db->GetIterator();
    while (it->isValid && it->error == 0) {
        char print[21];
        int len = 20;
        if (it->len < 20) {
            len = it->len;
        }
        memcpy(print, it->data, len);
        print[len+1] = '\0';
        printf("Test: CallForEach: %s\n", print);
        ++(*it);
    }

    it = db->GetIterator();
    while (it->isValid && it->error == 0) {
        if (db->IsMostRecentEntry(it->GetIdentifier(), it->fawnds_id, it->data,
                                  it->len) == 0) {
            char print[21];
            int len = 20;
            if (it->len < 20) {
                len = it->len;
            }
            memcpy(print, it->data, len);
            print[len+1] = '\0';
            printf("Test: CallForEach2: %s\n", print);
        }
        ++(*it);
    }
    */
    delete db;
}
