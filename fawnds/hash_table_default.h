/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _HASH_TABLE_DEFAULT_H_
#define _HASH_TABLE_DEFAULT_H_

#include "basic_types.h"
#include "fawnds.h"
#include <tbb/spin_mutex.h>

namespace fawn {

class Configuration;

// configuration
//   <type>: "default" (fixed)
//   <id>: the ID of the file store
//   <file>: the file name prefix to store the hash table for startup 
//   <hash-table-size>: the number of entries that the hash table is expected to hold
//   <probing-mode>: "linear" (default) for linear probing
//                   "quadratic" for quadratic probing

class HashTableDefault : public FawnDS {
    // Incrementing keyfragbits above 15 requires
    // more modifications to code (e.g. hashkey is 16 bits in (Insert())
    static const uint32_t KEYFRAGBITS = 15;
    static const uint32_t KEYFRAGMASK = (1 << KEYFRAGBITS) - 1;
    static const uint32_t INDEXBITS = 16;
    static const uint32_t INDEXMASK = (1 << INDEXBITS) - 1;
    static const uint32_t VALIDBITMASK = KEYFRAGMASK+1;

    static const double EXCESS_BUCKET_FACTOR = 1.1;
    static const double MAX_DELETED_RATIO = 0.8;
    static const double MAX_LOAD_FACTOR = 0.9;

    static const double PROBES_BEFORE_REHASH = 8;
    //static const double PROBES_BEFORE_REHASH = 16;

protected:
    /*
      Hash Entry Format
      D = Is slot deleted: 1 means deleted, 0 means not deleted.  Needed for lazy deletion
      V = Is slot empty: 0 means empty, 1 means taken
      K = Key fragment
      O = Offset bits
      ________________________________________________
      |DVKKKKKKKKKKKKKKOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO|
      ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯
    */
    struct HashEntry {
        uint16_t present_key;
        uint32_t id;
    } __attribute__((__packed__));


public:
    HashTableDefault();
    virtual ~HashTableDefault();

    virtual FawnDS_Return Create();
    virtual FawnDS_Return Open();

    virtual FawnDS_Return ConvertTo(FawnDS* new_store) const;

    virtual FawnDS_Return Flush();
    virtual FawnDS_Return Close();

    virtual FawnDS_Return Destroy();

    virtual FawnDS_Return Status(const FawnDS_StatusType& type, Value& status) const;

    virtual FawnDS_Return Put(const ConstValue& key, const ConstValue& data);
    //virtual FawnDS_Return Append(Value& key, const ConstValue& data);

    //virtual FawnDS_Return Delete(const ConstValue& key);

    virtual FawnDS_Return Contains(const ConstValue& key) const;
    virtual FawnDS_Return Length(const ConstValue& key, size_t& len) const;
    virtual FawnDS_Return Get(const ConstValue& key, Value& data, size_t offset = 0, size_t len = -1) const;

    virtual FawnDS_ConstIterator Enumerate() const;
    virtual FawnDS_Iterator Enumerate();

    virtual FawnDS_ConstIterator Find(const ConstValue& key) const;
    virtual FawnDS_Iterator Find(const ConstValue& key);

    struct IteratorElem : public FawnDS_IteratorElem {
        FawnDS_IteratorElem* Clone() const;
        void Next();
        FawnDS_Return Replace(const ConstValue& data);

        uint16_t vkey;

        size_t current_probe_count;
        size_t max_probe_count;

        size_t next_hash_fn_index;

        size_t first_index;
        size_t current_index;

        bool skip_unused;
    };

protected:
    int WriteToFile();
    int ReadFromFile();

private:
    HashEntry* hash_table_;
    size_t hash_table_size_;
    size_t current_entries_;

    // probe increment function coefficients
    double c_1_;
    double c_2_;

    mutable tbb::spin_mutex cache_mutex_;
    mutable Value cached_lookup_key_;
    mutable size_t cached_unused_index_;

    inline bool valid(int32_t index) const {
        return (hash_table_[index].present_key & VALIDBITMASK);
    }

    inline uint16_t verifykey(int32_t index) const {
        return (hash_table_[index].present_key & KEYFRAGMASK);
    }

    inline uint16_t keyfragment_from_key(const ConstValue& key) const {
        if (key.size() < sizeof(uint16_t)) {
            return (uint16_t) (key.data()[0] & KEYFRAGMASK);
        }
        return (uint16_t) (((key.data()[key.size()-2]<<8) + (key.data()[key.size()-1])) & KEYFRAGMASK);
    }

    friend class IteratorElem;
};

} // namespace fawn

#endif // #ifndef _HASH_TABLE_DEFAULT_H_
