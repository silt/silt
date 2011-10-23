/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _SORTER_H_
#define _SORTER_H_

#include "fawnds.h"

#include "config.h"
#ifndef HAVE_LIBNSORT
#include <vector>
#else
#include <nsort.h>
#endif

namespace fawn {

    // configuration
    //   <type>: "sorter" (fixed)
    //   <key-len>: the length of keys -- zero for variable-length keys (default), a positive integer for fixed-length keys
    //   <data-len>: the length of data -- zero for variable-length data (default), a positive integer for fixed-length data
    //   <temp-file>: the path of the temporary files/directory. "/tmp" is the default.

    class Sorter : public FawnDS {
    public:
        Sorter();
        virtual ~Sorter();

        virtual FawnDS_Return Create();
        //virtual FawnDS_Return Open();

        virtual FawnDS_Return Flush();
        virtual FawnDS_Return Close();

        //virtual FawnDS_Return Destroy();

        virtual FawnDS_Return Put(const ConstValue& key, const ConstValue& data);
        //virtual FawnDS_Return Append(Value& key, const ConstValue& data);

        //virtual FawnDS_Return Delete(const ConstValue& key);

        //virtual FawnDS_Return Contains(const ConstValue& key) const;
        //virtual FawnDS_Return Length(const ConstValue& key, size_t& len) const;
        //virtual FawnDS_Return Get(const ConstValue& key, Value& data, size_t offset = 0, size_t len = -1) const;

        virtual FawnDS_ConstIterator Enumerate() const;
        virtual FawnDS_Iterator Enumerate();

        //virtual FawnDS_ConstIterator Find(const ConstValue& key) const;
        //virtual FawnDS_Iterator Find(const ConstValue& key);
    
        struct IteratorElem : public FawnDS_IteratorElem {
            IteratorElem(const Sorter* fawnds);

            FawnDS_IteratorElem* Clone() const;
            void Next();
            
            void Increment(bool initial);
        };

    protected:
        bool open_;
        bool input_ended_;

        size_t key_len_;
        size_t data_len_;

        std::string temp_file_;

#ifndef HAVE_LIBNSORT
        std::vector<Value> key_array_;
        std::vector<Value> data_array_;
        std::vector<size_t> refs_;
        mutable std::vector<size_t>::const_iterator refs_it_;
#else
        mutable nsort_t nsort_ctx_;
        mutable Value nsort_buf_;
#endif
    };

} // namespace fawn

#endif  // #ifndef _SORTER_H_
