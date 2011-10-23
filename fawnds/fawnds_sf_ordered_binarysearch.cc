#include "fawnds_sf_ordered_binarysearch.h"
#include <boost/functional/hash.hpp>    // for debugging (hash_state())

namespace fawn
{
    template <typename Store>
    FawnDS_SF_Ordered_BinarySearch<Store>*
    FawnDS_SF_Ordered_BinarySearch<Store>::Create(Configuration* const &config, const uint64_t &hash_table_size)
    {
        FawnDS_SF_Ordered_BinarySearch<Store>* fawnds = new FawnDS_SF_Ordered_BinarySearch<Store>(config);
        fawnds->Initialize(hash_table_size);
        return fawnds;
    }

    template <typename Store>
    FawnDS_SF_Ordered_BinarySearch<Store>* 
    FawnDS_SF_Ordered_BinarySearch<Store>::Open(Configuration* const &config)
    {
        FawnDS_SF_Ordered_BinarySearch<Store>* fawnds = new FawnDS_SF_Ordered_BinarySearch<Store>(config);
        fawnds->LoadFromFile();
        return fawnds;
    }

    template <typename Store>
    FawnDS_SF_Ordered_BinarySearch<Store>::FawnDS_SF_Ordered_BinarySearch(Configuration* const &config)
        : config_(config), initialized_(false)
    {
        pthread_rwlock_init(&fawnds_lock_, NULL);

        // TODO: range check
        key_len_ = atoi(config->getStringValue("datastore/key-len").c_str());
        data_len_ = atoi(config->getStringValue("datastore/data-len").c_str());

        Configuration* const storeConfig = new Configuration(config_);
        storeConfig->setPrefix(config_->getPrefix() + "/datastore");
        data_store_ = new Store(storeConfig);

        last_inserted_key_ = new char[key_len_];
    }

    template <typename Store>
    FawnDS_SF_Ordered_BinarySearch<Store>::~FawnDS_SF_Ordered_BinarySearch()
    {
        delete [] last_inserted_key_;
        delete data_store_;

        pthread_rwlock_destroy(&fawnds_lock_);
    }

    template <typename Store>
    void
    FawnDS_SF_Ordered_BinarySearch<Store>::Initialize(uint64_t hash_table_size)
    {
        if (initialized_) {
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: Initialize: reinitailization\n");
            return;
        }

        base_data_store_offset_ = data_store_->getCurrentOffset();
        hash_table_size_ = hash_table_size;
        actual_size_ = 0;

        finalized_ = false;

        initialized_ = true;
        fprintf(stdout, "FawnDS_SF_Ordered_BinarySearch: Initialize: initialized\n");
    }

    template <typename Store>
    void
    FawnDS_SF_Ordered_BinarySearch<Store>::LoadFromFile()
    {
        if (initialized_) {
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: LoadFromFile: already initialized\n");
            return;
        }

        string filename = config_->getStringValue("file");
        int fd;
        if ((fd = open(filename.c_str(), O_RDONLY | O_NOATIME, 0666)) == -1) {
            perror("FawnDS_SF_Ordered_BinarySearch: LoadFromFile: could not open index file");
            return;
        }
        
        // read base offset
        if (read(fd, &base_data_store_offset_, sizeof(base_data_store_offset_)) != sizeof(base_data_store_offset_)) {
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: LoadFromFile: unable to read base offset\n");
            close(fd);
            return;
        }

        // read hash table size
        if (read(fd, &hash_table_size_, sizeof(hash_table_size_)) != sizeof(hash_table_size_)) {
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: LoadFromFile: unable to read hash table size\n");
            close(fd);
            return;
        }

        // read actual hash table size
        if (read(fd, &actual_size_, sizeof(actual_size_)) != sizeof(actual_size_)) {
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: LoadFromFile: unable to read actual hash table size\n");
            close(fd);
            return;
        }

        close(fd);
        finalized_ = true;

        initialized_ = true;
        fprintf(stdout, "FawnDS_SF_Ordered_BinarySearch: LoadFromFile: loaded\n");

#ifdef DEBUG
        fprintf(stdout, "FawnDS_SF_Ordered_BinarySearch: LoadFromFile: state: %lu\n", hash_state());
#endif
    }

    template <typename Store>
    void
    FawnDS_SF_Ordered_BinarySearch<Store>::StoreToFile()
    {
        if (!initialized_) {
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: StoreToFile: not initailized\n");
            return;
        }

        if (!finalized_) {
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: StoreToFile: not finalized\n");
            return;
        }

        string filename = config_->getStringValue("file");
        int fd;
        if ((fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_NOATIME, 0666)) == -1) {
            perror("FawnDS_SF_Ordered_BinarySearch: StoreToFile: could not open index file");
            return;
        }

        // write base offset
        if (write(fd, &base_data_store_offset_, sizeof(base_data_store_offset_)) != sizeof(base_data_store_offset_)) {
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: StoreToFile: unable to write base offset\n");
            close(fd);
            return;
        }

        // write hash table size
        if (write(fd, &hash_table_size_, sizeof(hash_table_size_)) != sizeof(hash_table_size_)) {
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: StoreToFile: unable to write hash table size\n");
            close(fd);
            return;
        }

        // write actual hash table size
        if (write(fd, &actual_size_, sizeof(actual_size_)) != sizeof(actual_size_)) {
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: StoreToFile: unable to write actual hash table size\n");
            close(fd);
            return;
        }

        close(fd);
        fprintf(stdout, "FawnDS_SF_Ordered_BinarySearch: StoreToFile: stored\n");

#ifdef DEBUG
        fprintf(stdout, "FawnDS_SF_Ordered_BinarySearch: StoreToFile: state: %lu\n", hash_state());
#endif
    }

    template <typename Store>
    int 
    FawnDS_SF_Ordered_BinarySearch<Store>::Insert(const char* const &key,
                                    const uint32_t &key_len, 
                                    const char* const &data, 
                                    const uint32_t &data_len)
    {
#ifdef DEBUG
        if (debug_level & 2) {
            if (key_len != key_len_)
                fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: Insert: invalid key_len\n");
            if (data_len != data_len_)
                fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: Insert: invalid data_len\n");
            if (key == NULL)
                fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: Insert: key is NULL!\n");
            if (data == NULL)
                fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: Insert: data is NULL!\n");
        }
#endif

        DPRINTF(2, "FawnDS_SF_Ordered_BinarySearch: Called Insert for key with length: %i at mem: %i and data: %s with length: %i at mem: %i ...", key_len, key, data, data_len, data);

        if (key == NULL || data == NULL || key_len != key_len_ || data_len != data_len_) {
            return -1;
        }

        pthread_rwlock_wrlock(&fawnds_lock_);

        if (finalized_)
        {
            pthread_rwlock_unlock(&fawnds_lock_);
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: Insert: already finalized\n");
            return -1;
        }

        if (actual_size_ > 0 && memcmp(last_inserted_key_, key, key_len) >= 0)
        {
            pthread_rwlock_unlock(&fawnds_lock_);
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: Insert: duplicate or not sorted key\n");
            return -1;
        }
        memcpy(last_inserted_key_, key, key_len);
        
        // put key-data to datastore
        off_t data_store_offset = data_store_->getCurrentOffset();
        if (data_store_->Put(data_store_offset, key_len, data_len, key, data) != 0) {
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: Insert: failed to Put to datastore\n");
            pthread_rwlock_unlock(&fawnds_lock_);
            return -1;
        }

        actual_size_++;

        pthread_rwlock_unlock(&fawnds_lock_);
        DPRINTF(2, "Inserted!\n");

        return 0;
    }
    
    template <typename Store>
    int
    FawnDS_SF_Ordered_BinarySearch<Store>::Get(const char* const &key, const uint32_t &key_len, char* &data, uint32_t &data_len)
    {
#ifdef DEBUG
        if (debug_level & 2) {
            if (key_len != key_len_)
                fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: Get: invalid key_len\n");
            if (key == NULL)
                fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: Get: key is NULL!\n");
        }
#endif

        DPRINTF(2, "FawnDS_SF_Ordered_BinarySearch: Called Get for key with length: %i at mem: %i ...", key_len, key);

        if (key == NULL || key_len != key_len_) {
            return false;
        }

        pthread_rwlock_rdlock(&fawnds_lock_);

        if (!finalized_)
        {
            pthread_rwlock_unlock(&fawnds_lock_);
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: Get: not finalized\n");
            return -1;
        }
        
        std::size_t key_index = lookup_key_index(key, 0, actual_size_);
        if (key_index != static_cast<std::size_t>(-1))
        {
            // TODO: hardcoded record size
            data_len = data_len_;
            off_t datapos = base_data_store_offset_ + key_index * (key_len_ + data_len_);

            //printf("reading %lu\n", datapos);
            data = new char[data_len];
            if(data_store_->RetrieveData(datapos, key_len, data_len, data) < 0) {
                delete [] data;
                pthread_rwlock_unlock(&fawnds_lock_);
                return -1;
            }
            pthread_rwlock_unlock(&fawnds_lock_);
            DPRINTF(2, "Get! Data: %s\n", data);
            return 0;
        }
            
        pthread_rwlock_unlock(&fawnds_lock_);
        fprintf(stderr, "Can't find data for given key.\n");
        return -1;
    }

    template <typename Store>
    int
    FawnDS_SF_Ordered_BinarySearch<Store>::Delete(const char* const &key __attribute__ ((unused)), const uint32_t &key_len __attribute__ ((unused)))
    {
        fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: Delete: not supported\n");
        return -1;
    }

    /*
    template <typename Store>
    int
    FawnDS_SF_Ordered_BinarySearch<Store>::DoForEach(void (*callForEach)(const uint32_t &key_len, const uint32_t &data_len, const char* const &key, const char* const &data, const bool &isDeleted), void (*callBeforeRest)()) const
    {
        return data_store_->DoForEach(callForEach, callBeforeRest);
    }
    */
    
    template <typename Store>
    int
    FawnDS_SF_Ordered_BinarySearch<Store>::Flush()
    {
        pthread_rwlock_wrlock(&fawnds_lock_);

        if (finalized_) {
            pthread_rwlock_unlock(&fawnds_lock_);
            fprintf(stderr, "FawnDS_SF_Ordered_BinarySearch: Flush: already finalized\n");
            return -1;
        }

        finalize();

        if (!finalized_)
        {
            pthread_rwlock_unlock(&fawnds_lock_);
            return -1;
        }

        pthread_rwlock_unlock(&fawnds_lock_);
        return 0;
    }
    
    template <typename Store>
    int
    FawnDS_SF_Ordered_BinarySearch<Store>::Destroy()
    {
        return data_store_->Destroy();
    }

    template <typename Store>
    FawnDSIter
    FawnDS_SF_Ordered_BinarySearch<Store>::IteratorBegin() const {
        return data_store_->IteratorBegin();
    }

    template <typename Store>
    std::size_t
    FawnDS_SF_Ordered_BinarySearch<Store>::lookup_key_index(const char* key, uint64_t off, int64_t n) const
    {
        //printf("binary search %lu %ld\n", off, n);

        if (n > 0) {
            int64_t n2 = n / 2;
            uint64_t mid = off + n2;

            uint32_t ckey_len;
            uint32_t cdata_len;
            bool cisDeleted;

            // TODO: hardcoded record size
            off_t datapos = base_data_store_offset_ + mid * (key_len_ + data_len_);

            //printf("reading %lu\n", datapos);
            if (data_store_->RetrieveMetadata(datapos, ckey_len, cdata_len, cisDeleted) == 0 && ckey_len == key_len_ && cisDeleted == false) {
                char ckey[ckey_len];
                if(data_store_->RetrieveKey(datapos, key_len_, cdata_len, ckey) == 0) {
                    int cmp = memcmp(key, ckey, key_len_);
                    //printf("cmp %d\n", cmp);
                    if (cmp < 0)
                        return lookup_key_index(key, off, n2);
                    else if (cmp > 0)
                        return lookup_key_index(key, mid + 1, n - (n2 + 1));
                    else
                        return mid;
                }
            }
        }

        return static_cast<std::size_t>(-1);
    }

    template <typename Store>
    void
    FawnDS_SF_Ordered_BinarySearch<Store>::finalize()
    {
        finalized_ = true;

        StoreToFile();
    }

    template <typename Store>
    std::size_t
    FawnDS_SF_Ordered_BinarySearch<Store>::hash_state() const
    {
        std::size_t sum = 0;

        boost::hash_combine(sum, initialized_);
        boost::hash_combine(sum, finalized_);

        boost::hash_combine(sum, key_len_);
        boost::hash_combine(sum, data_len_);

        boost::hash_combine(sum, base_data_store_offset_);
        boost::hash_combine(sum, hash_table_size_);
        boost::hash_combine(sum, actual_size_);

        return sum;
    }


    template class FawnDS_SF_Ordered_BinarySearch<FileStore_bare>;
}  // namespace fawn

