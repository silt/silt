/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNDS_PARTITION_H_
#define _FAWNDS_PARTITION_H_

#include "fawnds.h"
#include <vector>

namespace fawn {

    // configuration
    //   <type>: "partition" (fixed)
    //   <id>: the ID of the store
    //   <skip-bits>: the number of MSBs to ignore when calculating the partition number
    //   <partitions>: the number of partitions; must be power of 2
    //   <store>: the configuration for the lower-level store
    //            <id>: will be assigned by FawnDS_Partition

    class FawnDS_Partition : public FawnDS {
    public:
        FawnDS_Partition();
        virtual ~FawnDS_Partition();

        virtual FawnDS_Return Create();
        virtual FawnDS_Return Open();

        virtual FawnDS_Return Flush();
        virtual FawnDS_Return Close();

        virtual FawnDS_Return Destroy();

        virtual FawnDS_Return Status(const FawnDS_StatusType& type, Value& status) const;

        virtual FawnDS_Return Put(const ConstValue& key, const ConstValue& data);
        virtual FawnDS_Return Append(Value& key, const ConstValue& data);

        virtual FawnDS_Return Delete(const ConstValue& key);

        virtual FawnDS_Return Contains(const ConstValue& key) const;
        virtual FawnDS_Return Length(const ConstValue& key, size_t& len) const;
        virtual FawnDS_Return Get(const ConstValue& key, Value& data, size_t offset = 0, size_t len = -1) const;

        virtual FawnDS_ConstIterator Enumerate() const;
        virtual FawnDS_Iterator Enumerate();

        virtual FawnDS_ConstIterator Find(const ConstValue& key) const;
        virtual FawnDS_Iterator Find(const ConstValue& key);

        struct IteratorElem : public FawnDS_IteratorElem {
            IteratorElem(const FawnDS_Partition* fawnds);
            ~IteratorElem();

            FawnDS_IteratorElem* Clone() const;
            void Next();

            size_t next_store;
            FawnDS_Iterator store_it;
        };

    private:
        FawnDS_Return alloc_stores();
        size_t get_partition(const ConstValue& key) const;

        size_t skip_bits_;
        size_t partitions_;
        size_t partition_bits_;

        std::vector<FawnDS*> stores_;

        friend class IteratorElem;
    };

} // namespace fawn

#endif  // #ifndef _FAWNDS_PARTITION_H_
