/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNDS_PROXY_H_
#define _FAWNDS_PROXY_H_

#include "fawnds.h"

namespace fawn {

    // configuration
    //   <type>: "proxy" (fixed)
    //   <(others)>: used by the main store

    class FawnDS_Proxy : public FawnDS {
    public:
        FawnDS_Proxy();
        virtual ~FawnDS_Proxy();

        virtual FawnDS_Return Create();
        virtual FawnDS_Return Open();

        virtual FawnDS_Return ConvertTo(FawnDS* new_store) const;

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

    protected:
        FawnDS* store_;
    };

} // namespace fawn

#endif  // #ifndef _FAWNDS_PROXY_H_
