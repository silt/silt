/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "hash_table_default.h"
#include "hash_functions.h"
#include "hashutil.h"
#include "configuration.h"
#include "print.h"
#include "file_io.h"
#include "debug.h"

#include <cstdio>
#include <cerrno>
#include <sstream>

namespace fawn {

    HashTableDefault::HashTableDefault()
        : hash_table_(NULL), hash_table_size_(0), c_1_(1.), c_2_(0.)
    {
    }

    HashTableDefault::~HashTableDefault()
    {
        if (hash_table_)
            Close();
    }

    FawnDS_Return
    HashTableDefault::Create()
    {
        if (hash_table_)
            Close();

        // Calculate hashtable size based on number of entries and other hashtable parameters
        // numObjects = num_entries - deleted_entries
        // size = max(numObjects * 2, hash_table_size)
        // this means hash_table_size is a lower bound on resizing.

        string hts_string = config_->GetStringValue("child::hash-table-size");
        int hts_int = atoi(hts_string.c_str());
        uint64_t hash_table_size = (uint64_t)hts_int;

        DPRINTF(2, "HashTableDefault::Create(): given hash_table_size=%llu\n", static_cast<long long unsigned>(hash_table_size));

        uint64_t numObjects = hash_table_size * EXCESS_BUCKET_FACTOR;
        uint64_t max_entries = HashUtil::FindNextHashSize(numObjects);

        DPRINTF(2, "CreateFawnDS table information:\n"
               "\t hashtablesize: %"PRIu64"\n"
               "\t num_entries: %i\n"
               "\t Maximum number of entries: %"PRIu64"\n",
               numObjects, 0, max_entries);

        hash_table_size_ = max_entries;
        current_entries_ = 0;

        string pm_string = config_->GetStringValue("child::probing-mode");
        if (pm_string == "" || pm_string == "linear") {
            c_1_ = 1.;
            c_2_ = 0.;
        }
        else if (pm_string == "quadratic") {
            c_1_ = 0.5;
            c_2_ = 0.5;
        }
        else {
            DPRINTF(2, "HashTableDefault::Create(): <result> invalid probing mode %s\n", pm_string.c_str());
            return ERROR;
        }

        DPRINTF(2, "HashTableDefault::Create(): <result> actual hash_table_size=%llu\n", static_cast<long long unsigned>(hash_table_size));

        hash_table_ = new HashEntry[hash_table_size_];
        // zero out the buffer
        memset(hash_table_, 0, sizeof(HashEntry) * hash_table_size_);
        DPRINTF(2, "HashTableDefault::Create(): <result> byte size=%zu\n", sizeof(HashEntry) * hash_table_size_);

        cached_lookup_key_.resize(0);
        cached_unused_index_ = -1;

        return OK;
    }

    FawnDS_Return
    HashTableDefault::Open()
    {
        if (hash_table_)
            Close();

        if (Create() == OK)
            if (ReadFromFile() == 0)
                return OK;

        cached_lookup_key_.resize(0);
        cached_unused_index_ = -1;

        return ERROR;
    }

    FawnDS_Return
    HashTableDefault::ConvertTo(FawnDS* new_store) const
    {
        HashTableDefault* table = dynamic_cast<HashTableDefault*>(new_store);
        if (!table)
            return UNSUPPORTED;

        if (hash_table_size_ != table->hash_table_size_) {
            fprintf(stderr, "HashTableDefault::ConvertTo(): hash table size mismatch\n");
            return ERROR;
        }

        if (c_1_ != table->c_1_) {
            fprintf(stderr, "HashTableDefault::ConvertTo(): hash table parameter mismatch\n");
            return ERROR;
        }

        if (c_2_ != table->c_2_) {
            fprintf(stderr, "HashTableDefault::ConvertTo(): hash table parameter mismatch\n");
            return ERROR;
        }

        memcpy(table->hash_table_, hash_table_, sizeof(HashEntry) * hash_table_size_);
        table->current_entries_ = current_entries_;

        return OK;
    }


    FawnDS_Return
    HashTableDefault::Flush()
    {
        if (WriteToFile())
            return ERROR;
        else
            return OK;
    }

    FawnDS_Return
    HashTableDefault::Close()
    {
        Flush();

        if (hash_table_)
            delete [] hash_table_;
        DPRINTF(2, "HashTableDefault::Close(): HashTable deleted\n");
        hash_table_ = NULL;
        return OK;
    }

    FawnDS_Return
    HashTableDefault::Destroy()
    {
        std::string filename = config_->GetStringValue("child::file") + "_";
        filename += config_->GetStringValue("child::id");

        if (unlink(filename.c_str())) {
            perror("Could not delete file");
            return ERROR;
        }

        return OK;
    }

    FawnDS_Return
    HashTableDefault::Status(const FawnDS_StatusType& type, Value& status) const
    {
        if (!hash_table_)
            return ERROR;

        std::ostringstream oss;
        switch (type) {
        case NUM_DATA:
            oss << hash_table_size_;
            break;
        case NUM_ACTIVE_DATA:
            oss << current_entries_;
            break;
        case CAPACITY:
            oss << hash_table_size_;
            break;
        case MEMORY_USE:
            oss << sizeof(struct HashEntry) * hash_table_size_;
            break;
        case DISK_USE:
            oss << sizeof(current_entries_) + sizeof(struct HashEntry) * hash_table_size_;
            break;
        default:
            return UNSUPPORTED;
        }
        status = NewValue(oss.str());
        return OK;
    }

    FawnDS_Return
    HashTableDefault::Put(const ConstValue& key, const ConstValue& data)
    {
#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "HashTableDefault::Put(): key=\n");
            print_payload((const u_char*)key.data(), key.size(), 4);
            DPRINTF(2, "HashTableDefault::Put(): data=%llu\n", static_cast<long long unsigned>(data.as_number<size_t>()));
        }
#endif

        uint16_t new_vkey = keyfragment_from_key(key);

        uint32_t new_id = data.as_number<uint32_t>(-1);
        if (new_id == static_cast<uint32_t>(-1)) {
            DPRINTF(2, "HashTableDefault::Put(): could not parse data as ID\n");
            return INVALID_DATA;
        }

        size_t unused_index;
        {
            tbb::spin_mutex::scoped_lock lock(cache_mutex_);

            if (cached_unused_index_ != static_cast<size_t>(-1) && cached_lookup_key_ == key) {
                DPRINTF(2, "HashTableDefault::Put(): using cached index for unused space\n");

                unused_index = cached_unused_index_;

                cached_lookup_key_.resize(0);
                cached_unused_index_ = -1;
            }
            else {
                lock.release();

                DPRINTF(2, "HashTableDefault::Put(): no cached index for unused space\n");
                FawnDS_Iterator it = Find(key);

                while (!it.IsEnd())
                    ++it;

                const IteratorElem& elem = static_cast<const IteratorElem&>(*it);

                if (valid(elem.current_index)) {
                    DPRINTF(2, "HashTableDefault::Put(): <result> no more space to put new key\n");
                    return INSUFFICIENT_SPACE;
                }

                unused_index = elem.current_index;
            }
        }

        hash_table_[unused_index].present_key = new_vkey | VALIDBITMASK;
        hash_table_[unused_index].id = new_id;

        current_entries_++;
        DPRINTF(2, "HashTableDefault::Put(): <result> added\n");
        return OK;
    }

    FawnDS_Return
    HashTableDefault::Contains(const ConstValue& key) const
    {
#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "HashTableDefault::Contains(): key=\n");
            print_payload((const u_char*)key.data(), key.size(), 4);
        }
#endif
        FawnDS_ConstIterator it = Find(key);
        if (!it.IsEnd()) {
            DPRINTF(2, "HashTableDefault::Contains(): <result> found\n");
            return OK;
        }
        else {
            DPRINTF(2, "HashTableDefault::Contains(): <result> not found\n");
            return KEY_NOT_FOUND;
        }
    }

    FawnDS_Return
    HashTableDefault::Length(const ConstValue& key, size_t& len) const
    {
#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "HashTableDefault::Length(): key=\n");
            print_payload((const u_char*)key.data(), key.size(), 4);
        }
#endif
        len = sizeof(uint32_t);    // HashEntry.id
        DPRINTF(2, "HashTableDefault::Length(): <result> len=%llu\n", static_cast<long long unsigned>(len));
        return Contains(key);
    }

    FawnDS_Return
    HashTableDefault::Get(const ConstValue& key, Value& data, size_t offset, size_t len) const
    {
#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "HashTableDefault::Get(): key=\n");
            print_payload((const u_char*)key.data(), key.size(), 4);
        }
#endif
        // this is not very useful due to key collisions, but let's just have it
        FawnDS_ConstIterator it = Find(key);
        if (!it.IsEnd()) {
            if (offset == 0 && len == static_cast<size_t>(-1))
                data = it->data;
            else {
                if (offset > it->data.size()) {
                    data.resize(0);
                    return OK;
                }

                if (offset + len > it->data.size())
                    len = it->data.size() - offset;

                data = NewValue(it->data.data() + offset, len);
            }
#ifdef DEBUG
            if (debug_level & 2) {
                DPRINTF(2, "HashTableDefault::Get(): <result> data=\n");
                print_payload((const u_char*)data.data(), data.size(), 4);
            }
#endif
            return OK;
        }
        else {
            DPRINTF(2, "HashTableDefault::Get(): <result> not found\n");
            return KEY_NOT_FOUND;
        }
    }

    FawnDS_ConstIterator
    HashTableDefault::Enumerate() const
    {
        IteratorElem* elem = new IteratorElem();
        elem->fawnds = this;
        elem->vkey = 0;

        elem->current_probe_count = 0;
        elem->max_probe_count = hash_table_size_;

        elem->next_hash_fn_index = Hashes::HASH_COUNT;

        elem->first_index = elem->current_index = 0;

        elem->skip_unused = true;

        elem->Next();
        return FawnDS_ConstIterator(elem);
    }

    FawnDS_Iterator
    HashTableDefault::Enumerate()
    {
        IteratorElem* elem = new IteratorElem();
        elem->fawnds = this;
        elem->vkey = 0;

        elem->current_probe_count = 0;
        elem->max_probe_count = hash_table_size_;

        elem->next_hash_fn_index = Hashes::HASH_COUNT;

        elem->first_index = elem->current_index = 0;

        elem->skip_unused = true;

        elem->Next();
        return FawnDS_Iterator(elem);
    }

    FawnDS_ConstIterator
    HashTableDefault::Find(const ConstValue& key) const
    {
        IteratorElem* elem = new IteratorElem();
        elem->fawnds = this;
        elem->key = key;
        elem->vkey = keyfragment_from_key(key);

        elem->current_probe_count = 0;
        elem->max_probe_count = 0;      // invokes the selection of the next hash function

        elem->next_hash_fn_index = 0;

        elem->first_index = elem->current_index = 0;

        elem->skip_unused = false;

        elem->Next();
        return FawnDS_ConstIterator(elem);
    }

    FawnDS_Iterator
    HashTableDefault::Find(const ConstValue& key)
    {
        IteratorElem* elem = new IteratorElem();
        elem->fawnds = this;
        elem->key = key;
        elem->vkey = keyfragment_from_key(key);

        elem->current_probe_count = 0;
        elem->max_probe_count = 0;      // invokes the selection of the next hash function

        elem->next_hash_fn_index = 0;

        elem->first_index = elem->current_index = 0;

        elem->skip_unused = false;

        elem->Next();
        return FawnDS_Iterator(elem);
    }

    FawnDS_IteratorElem*
    HashTableDefault::IteratorElem::Clone() const
    {
        IteratorElem* elem = new IteratorElem();
        *elem = *this;
        return elem;
    }

    void
    HashTableDefault::IteratorElem::Next()
    {
        const HashTableDefault* table = static_cast<const HashTableDefault*>(fawnds);

        while (true) {
            if (current_probe_count >= max_probe_count) {
                if (next_hash_fn_index == Hashes::HASH_COUNT) {
                    state = END;
                    break;
                }
                current_probe_count = 0;
                max_probe_count = PROBES_BEFORE_REHASH;
                first_index = (*(Hashes::hashes[next_hash_fn_index++]))(key.data(), key.size());
                first_index &= table->hash_table_size_ - 1;
            }

            current_probe_count++;

            double n_i = first_index +
                         table->c_1_ * current_probe_count +
                         table->c_2_ * current_probe_count * current_probe_count;
            current_index = static_cast<size_t>(n_i + 0.5);
            current_index &= table->hash_table_size_ - 1;

            DPRINTF(2, "HashTableDefault::IteratorElem::Next(): index %zu with probe count %zu\n", current_index, current_probe_count);

            if (table->valid(current_index)) {
                if (key.size() == 0 || table->verifykey(current_index) == vkey) {
                    data = RefValue(&table->hash_table_[current_index].id);
                    state = OK;
                    break;
                }
                else {
                    // keyfrag mismatch
                    continue;
                }
            }
            else {
                if (skip_unused)
                    continue;

                {
                    tbb::spin_mutex::scoped_lock lock(table->cache_mutex_);
                    table->cached_lookup_key_ = key;
                    table->cached_unused_index_ = current_index;
                }

                state = END;
                break;
            }
        }

#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "HashTableDefault::IteratorElem::Next(): <result> now at index %zu\n", current_index);
            if (state == OK) {
                DPRINTF(2, "HashTableDefault::IteratorElem::Next(): <result> key=\n");
                print_payload((const u_char*)key.data(), key.size(), 4);
                DPRINTF(2, "HashTableDefault::IteratorElem::Next(): <result> data=%llu\n", static_cast<long long unsigned>(data.as_number<size_t>()));
            }
        }
#endif
    }

    FawnDS_Return
    HashTableDefault::IteratorElem::Replace(const ConstValue& data)
    {
#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "HashTableDefault::IteratorElem::Replace(): index %zu, data=%llu\n", current_index, static_cast<long long unsigned>(data.as_number<size_t>()));
        }
#endif
        
        HashTableDefault* table = static_cast<HashTableDefault*>(const_cast<FawnDS*>(fawnds));

        uint32_t new_id = data.as_number<uint32_t>(-1);
        if (new_id == static_cast<uint32_t>(-1)) {
            DPRINTF(2, "HashTableDefault::IteratorElem::Replace(): could not parse data as ID\n");
            return INVALID_DATA;
        }

        DPRINTF(2, "HashTableDefault::IteratorElem::Replace(): <result> updated\n");
        table->hash_table_[current_index].id = new_id;
        return OK;
    }

    int
    HashTableDefault::WriteToFile()
    {
        uint64_t offset = 0;
        uint64_t length = sizeof(current_entries_) + sizeof(struct HashEntry) * hash_table_size_;

        std::string filename = config_->GetStringValue("child::file") + "_";
        filename += config_->GetStringValue("child::id");

        DPRINTF(2, "HashTableDefault::WriteToFile(): writing to %s\n", filename.c_str());
        int fd;
        if ((fd = open(filename.c_str(), O_RDWR|O_CREAT|O_NOATIME, 0666)) == -1)
            {
                perror("Could not open file");
                return -1;
            }

        if (ftruncate(fd, (off_t)length) == -1) {
            fprintf(stderr, "Could not extend file to %"PRIu64" bytes: %s\n",
                    length, strerror(errno));
        }
        lseek(fd, 0, SEEK_SET);

        if (fill_file_with_zeros(fd, length) < 0) {
            fprintf(stderr, "Could not zero out hash_table file.\n");
            return -1;
        }

        if (pwrite64(fd, &current_entries_, sizeof(current_entries_), offset) != sizeof(current_entries_)) {
            perror("Error in writing");
            return -1;
        }
        offset += sizeof(current_entries_);
        length -= sizeof(current_entries_);

        // write the hashtable to the file
        ssize_t nwrite;
        while ((nwrite = pwrite64(fd, hash_table_, length, offset)) > 0) {
            length -= nwrite;
            offset += nwrite;
            if (nwrite < 0) {
                perror("Error in writing");
                return -1;
            }
        }
        fdatasync(fd);
        if (close(fd) == -1)
            {
                perror("Could not close file");
                return -1;
            }
        return 0;
    }

    // This assumes that the file was closed properly.
    int
    HashTableDefault::ReadFromFile()
    {
        std::string filename = config_->GetStringValue("child::file") + "_";
        filename += config_->GetStringValue("child::id");

        DPRINTF(2, "HashTableDefault::ReadFromFile(): reading from %s\n", filename.c_str());
        int fd;
        if ((fd = open(filename.c_str(), O_RDWR|O_CREAT|O_NOATIME, 0666)) == -1)
            {
                perror("Could not open file");
                return -1;
            }

        uint64_t offset = 0;
        uint64_t length = sizeof(current_entries_) + sizeof(struct HashEntry) * hash_table_size_;

        if (pread64(fd, &current_entries_, sizeof(current_entries_), offset) != sizeof(current_entries_)) {
            perror("Error in reading hashtable from file\n");
            return -1;
        }
        offset += sizeof(current_entries_);
        length -= sizeof(current_entries_);

        if (hash_table_ == NULL) {
            perror("hash table is NULL\n");
            return -1;
        }
        ssize_t nread;
        while ((nread = pread64(fd, hash_table_, length, offset)) > 0) {
            length -= nread;
            offset += nread;
            if (nread < 0) {
                perror("Error in reading hashtable from file\n");
                return -1;
            }
        }
        if (close(fd) == -1)
            {
                perror("Could not close file");
                return -1;
            }
        return 0;
    }

} // namespace fawn
