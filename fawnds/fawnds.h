/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNDS_H_
#define _FAWNDS_H_

#include "basic_types.h"
#include "fawnds_types.h"
#include "value.h"
#include "fawnds_iterator.h"

namespace fawn {

    class Configuration;

    // common interface for FAWN-DS stores
    class FawnDS {
    public:
        FawnDS();
        virtual ~FawnDS();

        FawnDS_Return SetConfig(const Configuration* config);   // move
        const Configuration* GetConfig() const;

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
        const Configuration* config_;
    };

} // namespace fawn

#endif  // #ifndef _FAWNDS_H_
