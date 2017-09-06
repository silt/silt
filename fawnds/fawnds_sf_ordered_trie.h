/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNDS_SF_ORDERED_TRIE_H_
#define _FAWNDS_SF_ORDERED_TRIE_H_

#include "fawnds_factory.h"
#include "bucketing_index.hpp"
#include "twolevel_absoff_bucketing.hpp"

namespace fawn
{
    // configuration
    //   <type>: "sf_ordered_trie" (fixed)
    //   <id>: the ID of the store
    //   <file>: the file name prefix to store bookkeeping information for persistency
    //   <key-len>: the fixed length of all keys
    //   <data-len>: the fixed length of all data
    //   <keys-per-block>: the number of key-value pairs in a I/O block
    //   <size>: the maximum number of entries in the store (used for construction only)
    //   <bucket-size>: the number of entires in each bucket
    //   <skip-bits>: the number of MSBs to ignore when calculating the bucket number
    //   <datastore>: the configuration for the data store
    //                <id>: will be assigned by FawnDS_SF_Ordered_Trie
    //                <data-len>: will be set by FawnDS_SF_Ordered_Trie

    class FawnDS_SF_Ordered_Trie : public FawnDS
    {
    public:
        FawnDS_SF_Ordered_Trie();
        virtual ~FawnDS_SF_Ordered_Trie();

        virtual FawnDS_Return Create();
        virtual FawnDS_Return Open();

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

        //virtual FawnDS_ConstIterator Find(const ConstValue& key) const;
        //virtual FawnDS_Iterator Find(const ConstValue& key);

        struct IteratorElem : public FawnDS_IteratorElem {
            IteratorElem(const FawnDS_SF_Ordered_Trie* fawnds);

            FawnDS_IteratorElem* Clone() const;
            void Next();
            void Parse();

            FawnDS_Iterator data_store_it;
        };

    protected:
        FawnDS_Return WriteHeaderToFile() const;
        FawnDS_Return ReadHeaderFromFile();

	private:
        typedef cindex::bucketing_index<cindex::twolevel_absoff_bucketing<>, cindex::trie<true, 64> > index_type;
        index_type* index_;
        FawnDS* data_store_;

        size_t key_len_;
        size_t data_len_;
        size_t keys_per_block_;

        size_t size_;
        size_t bucket_size_;

        size_t skip_bits_;

        size_t actual_size_;
    };
}  // namespace fawn

#endif  // #ifndef _FAWNDS_SF_ORDERED_TRIE_H_
