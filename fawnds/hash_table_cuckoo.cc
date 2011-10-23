/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "hash_table_cuckoo.h"
#include "hash_functions.h"
#include "hashutil.h"
#include "configuration.h"
#include "print.h"
#include "file_io.h"
#include "debug.h"
#include "file_store.h"

#include <cstdio>
#include <cerrno>
#include <sstream>

namespace fawn {

    HashTableCuckoo::HashTableCuckoo()
        :hash_table_(NULL), fpf_table_(NULL)
    {
        DPRINTF(2, "%d-%d Cuckoo hash table\n", NUMHASH, ASSOCIATIVITY);
    }

    HashTableCuckoo::~HashTableCuckoo()
    {
        if (hash_table_ || fpf_table_)
            Close();
    }

    FawnDS_Return
    HashTableCuckoo::Create()
    {
        DPRINTF(2, "HashTableCuckoo::Create()\n");

        if (hash_table_ || fpf_table_)
            Close();

        string hts_string = config_->GetStringValue("child::hash-table-size");
        int hts_int = atoi(hts_string.c_str());
        if (hts_int <= 0) {
            return ERROR;
        }
        uint64_t table_size = (uint64_t)hts_int;

#ifdef SPLIT_HASHTABLE
        max_index_ = (1 << KEYFRAGBITS) * NUMHASH;
#else
        max_index_ = (1 << KEYFRAGBITS);
#endif

        if (max_index_ * ASSOCIATIVITY >  table_size)
            max_index_ = table_size / ASSOCIATIVITY;

        max_entries_ = ASSOCIATIVITY * max_index_;
        current_entries_ = 0;

        DPRINTF(2, "HashTableCuckoo::Create(): given table_size=%llu\n", static_cast<long long unsigned>(max_entries_));


        DPRINTF(2, "CreateFawnDS table information:\n"
                "\t KEYFRAGBITS: %ld\n"
                "\t hashtablesize: %ld\n"
                "\t num_entries: %ld\n"
                "\t Maximum number of entries: %ld\n",
                KEYFRAGBITS, max_index_, current_entires, max_entries_);

        if (config_->ExistsNode("child::use-offset") != 0 || atoi(config_->GetStringValue("child::use-offset").c_str()) != 0) {
            hash_table_ = new TagValStoreEntry[max_index_];
            fpf_table_  = NULL;

            // zero out the buffer
            memset(hash_table_, 0, sizeof(TagValStoreEntry) * max_index_);

            DPRINTF(2, "HashTableCuckoo::Create(): <result> byte size=%zu\n", sizeof(TagValStoreEntry) * max_index_);
        }
        else {
            hash_table_ = NULL;
            fpf_table_  = new TagStoreEntry[max_index_];

            // zero out the buffer
            memset(fpf_table_, 0, sizeof(TagStoreEntry) * max_index_);

            DPRINTF(2, "HashTableCuckoo::Create(): <result> byte size=%zu\n", sizeof(TagStoreEntry) * max_index_);
        }

        return OK;
    }

    FawnDS_Return
    HashTableCuckoo::Open()
    {
        DPRINTF(2, "HashTableCuckoo::Open()\n");
        if (hash_table_ || fpf_table_)
            Close();

        if (Create() == OK)
            if (ReadFromFile() == 0)
                return OK;

        return ERROR;
        
    }

    FawnDS_Return
    HashTableCuckoo::ConvertTo(FawnDS* new_store) const
    {
        HashTableCuckoo* new_cuckoo = dynamic_cast<HashTableCuckoo*>(new_store);
        if (!new_cuckoo) {
            fprintf(stderr, "HashTableCuckoo::ConvertTo(): <result> Don't know how to convert\n");
            return UNSUPPORTED;
        }

        if (max_index_ != new_cuckoo->max_index_) {
            fprintf(stderr, "HashTableDefault::ConvertTo(): hash table size mismatch\n");
            return ERROR;
        }

        if (hash_table_ == NULL && new_cuckoo->hash_table_ != NULL) {
            DPRINTF(2, "HashTableCuckoo::ConvertTo(): insufficient information for conversion\n");
            return ERROR;
        }

        DPRINTF(2, "HashTableCuckoo::ConvertTo(): converting to HashTableCuckoo\n");

        if (hash_table_ != NULL && new_cuckoo->hash_table_ != NULL)
            memcpy(new_cuckoo->hash_table_, hash_table_, sizeof(TagValStoreEntry) * max_index_);
        else if (hash_table_ == NULL && new_cuckoo->hash_table_ == NULL)
            memcpy(new_cuckoo->fpf_table_, fpf_table_, sizeof(TagStoreEntry) * max_index_);
        else {
            // copy bytes from h
            for (size_t i = 0; i < max_index_; i++) {
                memcpy(new_cuckoo->fpf_table_[i].tag_vector, hash_table_[i].tag_vector, sizeof(TagStoreEntry));
            }
        }
        new_cuckoo->current_entries_ = current_entries_;
        return OK;
    }

    FawnDS_Return
    HashTableCuckoo::Flush()
    {
        DPRINTF(2, "HashTableCuckoo::Flush()\n");
        if (WriteToFile())
            return ERROR;
        else
            return OK;

    }

    FawnDS_Return
    HashTableCuckoo::Close()
    {
        if (!hash_table_ && !fpf_table_)
            return ERROR;

        Flush();

        DPRINTF(2, "HashTableCuckoo::Close()\n");
        if (hash_table_) {
            delete [] hash_table_;
            DPRINTF(2, "HashTableCuckoo::Close(): HashTable deleted\n");
        }
        if (fpf_table_) {
            delete [] fpf_table_;
            DPRINTF(2, "HashTableCuckoo::Close(): FpfTable deleted\n");
        }
        hash_table_ = NULL;
        fpf_table_  = NULL;
        return OK;
    }

    FawnDS_Return
    HashTableCuckoo::Destroy()
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
    HashTableCuckoo::Status(const FawnDS_StatusType& type, Value& status) const
    {
        if (!hash_table_ && !fpf_table_)
            return ERROR;

        std::ostringstream oss;
        switch (type) {
        case NUM_DATA:
            oss << max_entries_;
            break;
        case NUM_ACTIVE_DATA:
            oss << current_entries_;
            break;
        case CAPACITY:
            oss << max_entries_;
            break;
        case MEMORY_USE:
            if (hash_table_)
                oss << sizeof(TagValStoreEntry) * max_index_;
            else if (fpf_table_)
                oss << sizeof(TagStoreEntry) * max_index_;
            else
                oss << 0;
            break;
        case DISK_USE:
            if (hash_table_)
                oss << sizeof(current_entries_) + sizeof(TagValStoreEntry) * max_index_;
            else if (fpf_table_)
                oss << sizeof(current_entries_) + sizeof(TagStoreEntry) * max_index_;
            else
                oss << sizeof(current_entries_);
            break;
        default:
            return UNSUPPORTED;
        }
        status = NewValue(oss.str());
        return OK;
    }

    FawnDS_Return
    HashTableCuckoo::Put(const ConstValue& key, const ConstValue& data)
    {
#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "HashTableCuckoo::Put(): key=\n");
            print_payload((const u_char*)key.data(), key.size(), 4);
            DPRINTF(2, "HashTableCuckoo::Put(): data=%llu\n", static_cast<long long unsigned>(data.as_number<size_t>()));
        }
#endif

        DPRINTF(2, "HashTableCuckoo::Put(): key=\n");
        //print_payload((const u_char*)key.data(), key.size(), 4);
        DPRINTF(2, "HashTableCuckoo::Put(): data=%llu\n", static_cast<long long unsigned>(data.as_number<size_t>()));
        
        // for undo correctness checking
        //uint32_t init_checksum = Hashes::h1(hash_table_, sizeof(TagValStoreEntry) * max_index_);

        uint32_t current_index, current_tag, current_val;
        uint32_t victim_way, victim_index, victim_tag, victim_val;
        uint32_t way;

        uint32_t undo_index[MAX_CUCKOO_COUNT];
        uint8_t undo_way[MAX_CUCKOO_COUNT];

        uint32_t fn = rand() % NUMHASH;
#ifdef SPLIT_HASHTABLE
        current_index = (keyfrag(key, fn) + fn * (1 << KEYFRAGBITS)) % max_index_;
#else
        current_index = keyfrag(key, fn) % max_index_;
#endif
        current_tag = keyfrag(key, (fn + 1) % NUMHASH) % max_index_;

        DPRINTF(2, "key: (index %d sig %d)\n", current_index, current_tag);

        current_val = static_cast<uint32_t> (data.as_number<uint32_t>());
        if ((way = freeslot(current_index)) < ASSOCIATIVITY) {
            /* an empty slot @ (index, way) of hash table */
            store(current_index, way, current_tag | VALIDBITMASK, current_val);
            current_entries_++;
            return OK;
        }

        fn = (fn + 1) % NUMHASH;
#ifdef SPLIT_HASHTABLE
        current_index = (keyfrag(key, fn) + fn * (1 << KEYFRAGBITS))  % max_index_;
#else
        current_index = keyfrag(key, fn)  % max_index_;
#endif
        current_tag = keyfrag(key, (fn + 1) % NUMHASH)  % max_index_;

        for (uint32_t n = 0; n < MAX_CUCKOO_COUNT; n++) {
            DPRINTF(2, "%d th try in loop!\n", n);
            DPRINTF(2, "key: (index %d sig %d)\n", current_index, current_tag);

            if ((way = freeslot(current_index)) < ASSOCIATIVITY) {
                /* an empty slot @ (current_index, current_way) of hash table */
                store(current_index, way, current_tag | VALIDBITMASK, current_val);
                current_entries_++;
                return OK;
            }

            victim_index = current_index;
            victim_way   = rand() % ASSOCIATIVITY;
            victim_tag   = tag(victim_index, victim_way);
            victim_val   = val(victim_index, victim_way);

            undo_index[n] = victim_index;
            undo_way[n] = victim_way;

            DPRINTF(2, "this bucket (%d) is full, take %d th as victim!\n", victim_index, victim_way);
            //DPRINTF(2, "victim: (index %d sig %d)\n", victim_index, victim_tag);
            store(victim_index, victim_way, current_tag | VALIDBITMASK, current_val);
#ifdef SPLIT_HASHTABLE
            current_index = (victim_tag + (1 << KEYFRAGBITS)) % max_index_;
            current_tag   = victim_index %  max_index_;
#else
            current_index = victim_tag  % max_index_;
            current_tag   = victim_index  % max_index_;
#endif
            current_val   = victim_val;
        }

        DPRINTF(2, "HashTableCuckoo::Put(): no more space to put new key -- undoing cuckooing\n");

        // undo cuckooing
        // restore the last state (find the previous victim_tag when calling last store())
#ifdef SPLIT_HASHTABLE
        current_tag   = (current_index + (1 << KEYFRAGBITS)) % max_index_;
#else
        current_tag   = current_index  % max_index_;
#endif
        for (uint32_t n = 0; n < MAX_CUCKOO_COUNT; n++) {
            victim_index  = undo_index[MAX_CUCKOO_COUNT - 1 - n];
            victim_way    = undo_way[MAX_CUCKOO_COUNT - 1 - n];
            victim_tag    = tag(victim_index, victim_way);
            victim_val    = val(victim_index, victim_way);

            DPRINTF(2, "restoring victim (index %d sig %d)\n", victim_index, current_tag);

            store(victim_index, victim_way, current_tag | VALIDBITMASK, current_val);

#ifdef SPLIT_HASHTABLE
            current_tag   = (victim_index + (1 << KEYFRAGBITS))  % max_index_;
#else
            current_tag   = victim_index  % max_index_;
#endif
            current_val   = victim_val;
        }

        assert(current_val == static_cast<uint32_t> (data.as_number<uint32_t>()));

        //uint32_t final_checksum = Hashes::h1(hash_table_, sizeof(TagValStoreEntry) * max_index_);
        //DPRINTF(2, "HashTableCuckoo::Put(): initial checksum=%u, final checksum=%u\n", init_checksum, final_checksum);

        DPRINTF(2, "HashTableCuckoo::Put(): <result> undoing done\n");

        return INSUFFICIENT_SPACE;
    } // HashTableCuckoo:Put()


    FawnDS_ConstIterator
    HashTableCuckoo::Enumerate() const
    {
        DPRINTF(2, "HashTableCuckoo::Enumerate() const\n");
        IteratorElem* elem = new IteratorElem();
        elem->fawnds = this;

        elem->current_index = static_cast<uint32_t>(-1);
        elem->current_way = ASSOCIATIVITY - 1;

        elem->Next();
        return FawnDS_ConstIterator(elem);
    }

    FawnDS_Iterator
    HashTableCuckoo::Enumerate()
    {
        DPRINTF(2, "HashTableCuckoo::Enumerate()\n");
        IteratorElem* elem = new IteratorElem();
        elem->fawnds = this;

        elem->current_index = static_cast<uint32_t>(-1);
        elem->current_way = ASSOCIATIVITY - 1;

        elem->Next();
        return FawnDS_Iterator(elem);
    }

    FawnDS_ConstIterator
    HashTableCuckoo::Find(const ConstValue& key) const
    {
        DPRINTF(2, "HashTableCuckoo::Find() const\n");
        IteratorElem* elem = new IteratorElem();
        elem->fawnds = this;
        elem->key = key;
        for (uint32_t i = 0 ; i < NUMHASH; i++)
            elem->keyfrag[i] = keyfrag(key, i)  % max_index_;

        elem->current_keyfrag_id = static_cast<uint32_t>(-1);
        elem->current_way = ASSOCIATIVITY - 1;

        elem->Next();
        return FawnDS_ConstIterator(elem);
    }

    FawnDS_Iterator
    HashTableCuckoo::Find(const ConstValue& key)
    {
        DPRINTF(2, "HashTableCuckoo::Find()\n");
        IteratorElem* elem = new IteratorElem();
        elem->fawnds = this;
        elem->key = key;
        for (uint32_t i = 0 ; i < NUMHASH; i++)
            elem->keyfrag[i] = keyfrag(key, i)  % max_index_;

        elem->current_keyfrag_id = static_cast<uint32_t>(-1);
        elem->current_way = ASSOCIATIVITY - 1;

        elem->Next();
        return FawnDS_Iterator(elem);
    }

    FawnDS_IteratorElem*
    HashTableCuckoo::IteratorElem::Clone() const
    {
        IteratorElem* elem = new IteratorElem();
        *elem = *this;
        return elem;
    }

    void
    HashTableCuckoo::IteratorElem::Next()
    {
        DPRINTF(2, "HashTableCuckoo::IteratorEnum::Next\n");
        const HashTableCuckoo* table = static_cast<const HashTableCuckoo*>(fawnds);

        bool cont = true;

        while (cont) {
            current_way ++;

            if (current_way >= ASSOCIATIVITY ) {
                // time to go to the next index
                current_way = 0;

                if (key.size() != 0) {
                    // Find()

                    current_keyfrag_id ++;        

                    if (current_keyfrag_id >= NUMHASH) {
                        state = END;
                        return;
                    }

#ifdef SPLIT_HASHTABLE
                    current_index = (table->keyfrag(key, current_keyfrag_id) + current_keyfrag_id * (1 << KEYFRAGBITS))  % table->max_index_;
#else
                    current_index = (table->keyfrag(key, current_keyfrag_id))  % table->max_index_;
#endif

                }
                else {
                    // Enumerate()

                    current_index++;

                    if (current_index >= table->max_index_) {
                        state = END;
                        return;
                    }
                }
            }

            // if (key.size() != 0) {
            //     // for Find(), current_index is derived from current_key_id
            // }

            assert(current_index < table->max_index_);

            DPRINTF(2, "check (row %d, col %d) ... ", current_index, current_way);

            if (table->valid(current_index, current_way)) {
                uint32_t tag = table->tag(current_index, current_way);
                DPRINTF(2,"tag=");
                //int_to_bit(tag, (KEYFRAGBITS ));

                if (key.size() != 0 && tag != keyfrag[(current_keyfrag_id + 1) % NUMHASH] ) {
                    // key mismatch
                    continue;
                }
                
            }
            else {
                // unused space
                continue;
            }

            state = OK;
            uint32_t v = table->val(current_index, current_way);        
            data = NewValue(&v);
            break;


        }
#ifdef DEBUG

        /*
        if (debug_level & 2) {
            DPRINTF(2, "HashTableCuckoo::IteratorElem::Next(): index %i with key %ld B @ %x data %ld B @ %x\n", current_index, key.size(), key.data(), data.size(), data.data());
            print_payload((const u_char*)key.data(), key.size());
            print_payload((const u_char*)data.data(), data.size());
        }
        */
#endif
        
    }

    FawnDS_Return
    HashTableCuckoo::IteratorElem::Replace(const ConstValue& data)
    {
#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "HashTableCuckoo::IteratorElem::Replace(): index %zu, data=%llu\n", current_index, static_cast<long long unsigned>(data.as_number<size_t>()));
        }
#endif
        HashTableCuckoo* table = static_cast<HashTableCuckoo*>(const_cast<FawnDS*>(fawnds));

        uint32_t new_id = data.as_number<uint32_t>(-1);
        if (new_id == static_cast<uint32_t>(-1)) {
            DPRINTF(2, "HashTableCuckoo::IteratorElem::Replace(): could not parse data as ID\n");
            return INVALID_DATA;
        }

        DPRINTF(2, "HashTableCuckoo::IteratorElem::Replace(): <result> updated\n");
        table->hash_table_[current_index].val_vector[current_way] = new_id;
        return OK;
    } // IteratorElem::Replace

    int
    HashTableCuckoo::WriteToFile()
    {
        uint64_t offset = 0;
        void* table = NULL;
        uint64_t length = 0;

        if (hash_table_) {
            table = hash_table_;
            length = sizeof(current_entries_) + sizeof(struct TagValStoreEntry) * max_index_;
        }
        else if (fpf_table_) {
            table = fpf_table_;
            length = sizeof(current_entries_) + sizeof(struct TagStoreEntry) * max_index_;
        }

        std::string filename = config_->GetStringValue("child::file") + "_";
        filename += config_->GetStringValue("child::id");
        DPRINTF(2, "HashTableCuckoo::WriteToFile(): writing to %s\n", filename.c_str());
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
        while ((nwrite = pwrite64(fd, table, length, offset)) > 0) {
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
    HashTableCuckoo::ReadFromFile()
    {
        std::string filename = config_->GetStringValue("child::file") + "_";
        filename += config_->GetStringValue("child::id");
        DPRINTF(2, "HashTableCuckoo::ReadFromFile(): reading from %s\n", filename.c_str());
        int fd;
        if ((fd = open(filename.c_str(), O_RDWR|O_CREAT|O_NOATIME, 0666)) == -1)
            {
                perror("Could not open file");
                return -1;
            }
        
        // TODO: support for fpf_table

        uint64_t offset = 0;
        uint64_t length = sizeof(current_entries_) + sizeof(struct TagValStoreEntry) * max_index_;

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
