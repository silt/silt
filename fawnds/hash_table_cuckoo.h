/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _HASH_TABLE_CUCKOO_H_
#define _HASH_TABLE_CUCKOO_H_

#include "basic_types.h"
#include "fawnds.h"

#include <cassert>
#include "print.h"
#include "debug.h"

#include <string>


//#define SPLIT_HASHTABLE

using namespace std;
namespace fawn {

class Configuration;

// configuration
//   <type>: "cuckoo" (fixed)
//   <id>: the ID of the file store
//   <file>: the file name prefix to store the hash table for startup 
//   <hash-table-size>: the number of entries that the hash table is expected to hold
//   <use-offset>: 1 (default): use an explicit offset field of 4 bytes
//                 0: do not use offsets; a location in the hash table becomes an offset

class HashTableCuckoo : public FawnDS {
    /*
     * parameters for cuckoo
     */
    static const uint32_t NUMHASH = 2;
    static const uint32_t ASSOCIATIVITY = 4;

    static const uint32_t NUMVICTIM = 2; // size of victim table
    static const uint32_t MAX_CUCKOO_COUNT = 128;
    /*
     * make sure KEYFRAGBITS + VALIDBITS <= 16
     */
    static const uint32_t KEYFRAGBITS = 15;
    static const uint32_t KEYFRAGMASK = (1 << KEYFRAGBITS) - 1;
    static const uint32_t VALIDBITMASK = KEYFRAGMASK + 1;
    static const uint32_t KEYPRESENTMASK =  VALIDBITMASK | KEYFRAGMASK;

protected:
    struct TagValStoreEntry {
        char     tag_vector[ASSOCIATIVITY * ( KEYFRAGBITS + 1) / 8];
        uint32_t val_vector[ASSOCIATIVITY];
    } __attribute__((__packed__));

    struct TagStoreEntry {
        char     tag_vector[ASSOCIATIVITY * ( KEYFRAGBITS + 1) / 8];
    } __attribute__((__packed__));

public:
    HashTableCuckoo();
    virtual ~HashTableCuckoo();

    virtual FawnDS_Return Create();
    virtual FawnDS_Return Open();

    virtual FawnDS_Return Flush();
    virtual FawnDS_Return Close();

    virtual FawnDS_Return Destroy();

    virtual FawnDS_Return Status(const FawnDS_StatusType& type, Value& status) const;

    virtual FawnDS_Return Put(const ConstValue& key, const ConstValue& data);
   
    virtual FawnDS_Return ConvertTo(FawnDS* new_store) const;


    // not supported by now.
    // virtual FawnDS_Return Get(const ConstValue& key, Value& data, size_t offset = 0, size_t len = -1) const;
    //virtual FawnDS_Return Append(Value& key, const ConstValue& data);
    //virtual FawnDS_Return Delete(const ConstValue& key);
    //virtual FawnDS_Return Contains(const ConstValue& key) const;
    //virtual FawnDS_Return Length(const ConstValue& key, size_t& len) const;

    virtual FawnDS_ConstIterator Enumerate() const;
    virtual FawnDS_Iterator Enumerate();

    virtual FawnDS_ConstIterator Find(const ConstValue& key) const;
    virtual FawnDS_Iterator Find(const ConstValue& key);

    struct IteratorElem : public FawnDS_IteratorElem {
        FawnDS_IteratorElem* Clone() const;
        void Next();
        FawnDS_Return Replace(const ConstValue& data);
        uint32_t keyfrag[NUMHASH];
        uint32_t current_keyfrag_id;
        uint32_t current_index;
        uint32_t current_way;
    };

protected:
    int WriteToFile();
    int ReadFromFile();

private:
    TagValStoreEntry* hash_table_;
    TagStoreEntry*    fpf_table_;

    uint32_t  max_index_;
    uint32_t  max_entries_;
    uint32_t  current_entries_;
    
    inline bool valid(uint32_t index, uint32_t way) const {
        uint32_t pos = way * (KEYFRAGBITS + 1);
        uint32_t offset = pos >> 3;
        uint32_t tmp;
        // this is not big-endian safe, and it accesses beyond the end of the table
        /*
        if (hash_table_)
           tmp = *((uint32_t *) (hash_table_[index].tag_vector + offset));
        else
           tmp = *((uint32_t *) (fpf_table_[index].tag_vector + offset));
        tmp = tmp >> (pos & 7);
        */
        // specialized code
        assert(KEYFRAGBITS == 15);
        if (hash_table_)
            tmp = *((uint16_t *) (hash_table_[index].tag_vector + offset));
        else
            tmp = *((uint16_t *) (fpf_table_[index].tag_vector + offset));
        return (tmp & VALIDBITMASK);
    }

    // read from tagvector in the hashtable
    inline uint32_t tag(uint32_t index, uint32_t way) const {
        uint32_t pos = way * (KEYFRAGBITS + 1);
        uint32_t offset = pos >> 3;
        uint32_t tmp;
        // this is not big-endian safe, and it accesses beyond the end of the table
        /*
        if (hash_table_)
            tmp = *((uint32_t *) (hash_table_[index].tag_vector + offset));
        else 
            tmp = *((uint32_t *) (fpf_table_[index].tag_vector + offset));
        tmp = tmp >> (pos & 7);
        */
        // specialized code
        assert(KEYFRAGBITS == 15);
        if (hash_table_)
            tmp = *((uint16_t *) (hash_table_[index].tag_vector + offset));
        else
            tmp = *((uint16_t *) (fpf_table_[index].tag_vector + offset));
        return (tmp & KEYFRAGMASK);
    }

    inline uint32_t val(uint32_t index, uint32_t way) const {
        if (hash_table_)
            return hash_table_[index].val_vector[way];
        else
            return index * ASSOCIATIVITY + way;
    }

    // store keyfrag + validbit to  tagvector in the hashtable
    void store(uint32_t index, uint32_t way, uint32_t keypresent, uint32_t id) {
        assert(hash_table_);
        assert(way < ASSOCIATIVITY);
        uint32_t pos = way * (KEYFRAGBITS + 1);
        uint32_t offset = pos >> 3;
        uint32_t shift = pos & 7;
        // this is not big-endian safe, and it accesses beyond the end of the table
        /*
        uint32_t *p= (uint32_t *) (hash_table_[index].tag_vector + offset);
        uint32_t tmp = *p;
        */
        // specialized code
        assert(KEYFRAGBITS == 15);
        uint16_t *p= (uint16_t *) (hash_table_[index].tag_vector + offset);
        uint16_t tmp = *p;
        tmp &= ~( KEYPRESENTMASK << shift);
        *p = tmp | ( keypresent << shift);;
        hash_table_[index].val_vector[way] = id;
    }



    /* read the keyfragment from the key */
    inline uint32_t keyfrag(const ConstValue& key, uint32_t keyfrag_id) const {
        assert(key.size() >= sizeof(uint32_t));
        // take the last 4 bytes
        uint32_t tmp = *((uint32_t *) (key.data() + key.size() - 4));
        tmp = (tmp >> (keyfrag_id * KEYFRAGBITS)) & KEYFRAGMASK;

#ifdef DEBUG
        // DPRINTF(2, "\t\t    key=\t");
        // print_payload((const u_char*) key.data(), key.size(), 4);
        // DPRINTF(2, "\t\t%dth tag=\t", keyfrag_index);
        // print_payload((const u_char*) &tmp, 2, 4);
#endif
        return tmp;
    }


    /* check if the given row has a free slot, return its way */
    uint32_t freeslot(uint32_t index)  {
        uint32_t way;
        for (way = 0; way < ASSOCIATIVITY; way++) {
            DPRINTF(4, "check ... (%d, %d)\t", index, way);
            if (!valid(index, way)) {
                DPRINTF(4, "not used!\n");
                return way;
            }
            DPRINTF(4, "used...\n");
        }
        return way;
    }

    friend class IteratorElem;
};

} // namespace fawn

#endif // #ifndef _HASH_TABLE_CUCKOO_H_
