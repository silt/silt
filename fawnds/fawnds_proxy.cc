/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "fawnds_proxy.h"
#include "fawnds_factory.h"
#include <cassert>

namespace fawn {

    FawnDS_Proxy::FawnDS_Proxy()
        : store_(NULL)
    {
    }

    FawnDS_Proxy::~FawnDS_Proxy()
    {
        if (store_)
            Close();
    }

    FawnDS_Return
    FawnDS_Proxy::Create()
    {
        if (store_)
            return ERROR;

        Configuration* store_config = new Configuration(config_, true);
        if (store_config->DeleteNode("child::type") != 0)
            assert(false);
        store_ = FawnDS_Factory::New(store_config);
        if (!store_) {
            delete store_config;
            return ERROR;
        }

        return store_->Create();
    }

    FawnDS_Return
    FawnDS_Proxy::Open()
    {
        if (store_)
            return ERROR;

        Configuration* store_config = new Configuration(config_, true);
        if (store_config->DeleteNode("child::type") != 0)
            assert(false);
        store_ = FawnDS_Factory::New(store_config);
        if (!store_) {
            delete store_config;
            return ERROR;
        }

        return store_->Open();
    }

    FawnDS_Return
    FawnDS_Proxy::ConvertTo(FawnDS* new_store) const
    {
        if (!store_)
            return ERROR;

        return store_->ConvertTo(new_store);
    }

    FawnDS_Return
    FawnDS_Proxy::Flush()
    {
        return store_->Flush();
    }

    FawnDS_Return
    FawnDS_Proxy::Close()
    {
        if (!store_)
            return ERROR;

        FawnDS_Return ret = store_->Close();
        if (ret == OK) {
            delete store_;
            store_ = NULL;
        }
        return ret;
    }

    FawnDS_Return
    FawnDS_Proxy::Destroy()
    {
        Configuration* store_config = new Configuration(config_, true);
        if (store_config->DeleteNode("child::type") != 0)
            assert(false);
        FawnDS* store = FawnDS_Factory::New(store_config);
        if (!store_) {
            delete store_config;
            return ERROR;
        }

        FawnDS_Return ret = store->Destroy();

        delete store;
        return ret;
    }

    FawnDS_Return
    FawnDS_Proxy::Status(const FawnDS_StatusType& type, Value& status) const
    {
        return store_->Status(type, status);
    }

    FawnDS_Return
    FawnDS_Proxy::Put(const ConstValue& key, const ConstValue& data)
    {
        return store_->Put(key, data);
    }

    FawnDS_Return
    FawnDS_Proxy::Append(Value& key, const ConstValue& data)
    {
        return store_->Append(key, data);
    }

    FawnDS_Return
    FawnDS_Proxy::Delete(const ConstValue& key)
    {
        return store_->Delete(key);
    }

    FawnDS_Return
    FawnDS_Proxy::Contains(const ConstValue& key) const
    {
        return store_->Contains(key);
    }

    FawnDS_Return
    FawnDS_Proxy::Length(const ConstValue& key, size_t& len) const
    {
        return store_->Length(key, len);
    }

    FawnDS_Return
    FawnDS_Proxy::Get(const ConstValue& key, Value& data, size_t offset, size_t len) const
    {
        return store_->Get(key, data, offset, len);
    }

    FawnDS_ConstIterator
    FawnDS_Proxy::Enumerate() const
    {
        return store_->Enumerate();
    }

    FawnDS_Iterator
    FawnDS_Proxy::Enumerate()
    {
        return store_->Enumerate();
    }

    FawnDS_ConstIterator
    FawnDS_Proxy::Find(const ConstValue& key) const
    {
        return store_->Find(key);
    }

    FawnDS_Iterator
    FawnDS_Proxy::Find(const ConstValue& key)
    {
        return store_->Find(key);
    }

} // namespace fawn
