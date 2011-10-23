/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNDS_SF_H_
#define _FAWNDS_SF_H_

#include "fawnds.h"

namespace fawn {

    // configuration
    //   <type>: "sf" (fixed)
    //   <id>: the ID of the store
    //   <file>: the file name prefix to store bookkeeping information for persistency
    //   <key-len>: the length of keys -- zero for variable-length keys (default), a positive integer for fixed-length keys (space optimization can be applied in the future)
    //   <data-len>: the length of data -- zero for variable-length data (default), a positive integer for fixed-length data (space optimization can be applied in the future)
    //   <no-delete>: omit support for deletion (not implemented yet)
    //   <keep-datastore-on-conversion>: when converting another FawnDS_SF store to this store, assume that the previous hash table preserves the same data layout in this data store; datastore->ConvertTo() is used instead of data reorganization based on side-by-side hash table comparison.
    //   <hashtable>: the configuration for the hash table
    //                <id>: will be assigned by FawnDS_SF
    //   <datastore>: the configuration for the insert-order-preserving data store
    //                <id>: will be assigned by FawnDS_SF
    //                <data-len>: will be set by FawnDS_SF (zero if either key length or data length is zero, (header size) + key length + data length otherwise)

    class FawnDS_SF : public FawnDS {
    public:
        FawnDS_SF();
        virtual ~FawnDS_SF();

        virtual FawnDS_Return Create();
        virtual FawnDS_Return Open();

        virtual FawnDS_Return ConvertTo(FawnDS* new_store) const;

        virtual FawnDS_Return Flush();
        virtual FawnDS_Return Close();

        virtual FawnDS_Return Destroy();

        virtual FawnDS_Return Status(const FawnDS_StatusType& type, Value& status) const;

        virtual FawnDS_Return Put(const ConstValue& key, const ConstValue& data);
        //virtual FawnDS_Return Append(Value& key, const ConstValue& data);

        virtual FawnDS_Return Delete(const ConstValue& key);

        virtual FawnDS_Return Contains(const ConstValue& key) const;
        virtual FawnDS_Return Length(const ConstValue& key, size_t& len) const;
        virtual FawnDS_Return Get(const ConstValue& key, Value& data, size_t offset = 0, size_t len = -1) const;

        virtual FawnDS_ConstIterator Enumerate() const;
        virtual FawnDS_Iterator Enumerate();

        //virtual FawnDS_ConstIterator Find(const ConstValue& key) const;
        //virtual FawnDS_Iterator Find(const ConstValue& key);

        struct IteratorElem : public FawnDS_IteratorElem {
            IteratorElem(const FawnDS_SF* fawnds);
            ~IteratorElem();

            FawnDS_IteratorElem* Clone() const;
            void Next();

            void Increment(bool initial);

            FawnDS_Iterator data_store_it;
        };

    protected:
        FawnDS_Return InsertEntry(const ConstValue& key, const ConstValue& data, bool isDelete);

        struct DbHeader {
            uint64_t magic_number;
            uint64_t num_elements;
            uint64_t num_active_elements;
            //double max_deleted_ratio;
            //double max_load_factor;
            //keyType keyFormat;
        } __attribute__((__packed__));

        struct DataHeaderFull {
            uint16_t type;  // 0 = unused, 1 = insert/update, 2 = delete
            uint16_t key_len;
        } __attribute__((__packed__));

        struct DataHeaderSimple {
            uint16_t type;  // 0 = unused, 1 = insert/update, 2 = delete
            uint16_t padding;
        } __attribute__((__packed__));

        /*
        static const int DSReadMin = 2048;
        struct DataHeaderExtended {
            //uint32_t data_length;
            uint32_t key_length;
            bool deleteLog;
            char partial_data[DSReadMin-sizeof(struct DataHeader)];
        } __attribute__((__packed__));
        */

    private:
        FawnDS_Return WriteHeaderToFile() const __attribute__ ((warn_unused_result));
        FawnDS_Return ReadHeaderFromFile() __attribute__ ((warn_unused_result));

        DbHeader* header_;
        FawnDS* hash_table_;

        FawnDS* data_store_;

        size_t key_len_;
        size_t data_len_;

        mutable pthread_rwlock_t fawnds_lock_;

        friend struct IteratorElem;
    };

}  // namespace fawn

#endif  // #ifndef _FAWNDS_SF_H_
