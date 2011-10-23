/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _BDB_H_
#define _BDB_H_

#include "fawnds.h"
#include "config.h"
#include <tbb/atomic.h>

#ifdef HAVE_LIBDB
#include <db.h>

namespace fawn {

    // configuration
    //   <type>: "bdb" (fixed)
    //   <id>: the ID of the store
    //   <file>: the file name prefix of Berkeley Database

    class BDB : public FawnDS {
    public:
        BDB();
        virtual ~BDB();

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

        virtual FawnDS_ConstIterator Find(const ConstValue& key) const;
        virtual FawnDS_Iterator Find(const ConstValue& key);

        struct IteratorElem : public FawnDS_IteratorElem {
            IteratorElem(const BDB* fawnds);
            ~IteratorElem();

            FawnDS_IteratorElem* Clone() const;
            void Next();

            void Increment(bool initial);

            DBC* cursor;
        };

    private:
        DB* dbp_;

        size_t key_len_;
        size_t data_len_;

        tbb::atomic<size_t> size_;

        friend struct IteratorElem;
    };

}  // namespace fawn

#endif

#endif  // #ifndef _FAWNDS_SF_H_
