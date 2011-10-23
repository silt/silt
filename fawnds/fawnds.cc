/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "fawnds.h"
#include "configuration.h"
#include <iostream>

namespace fawn {

    FawnDS::FawnDS()
        : config_(NULL)
    {
    }

    FawnDS::~FawnDS()
    {
        delete config_;
        config_ = NULL;
    }

    FawnDS_Return
    FawnDS::SetConfig(const Configuration* config)
    {
        if (config_)
            return ERROR;
        config_ = config;
        return OK;
    }

    const Configuration*
    FawnDS::GetConfig() const
    {
        return config_;
    }

    FawnDS_Return
    FawnDS::Create()
    {
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS::Open()
    {
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS::ConvertTo(FawnDS* new_store) const
    {
        (void)new_store;
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS::Flush()
    {
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS::Close()
    {
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS::Destroy()
    {
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS::Status(const FawnDS_StatusType& type, Value& status) const
    {
        (void)type; (void)status;
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS::Put(const ConstValue& key, const ConstValue& data)
    {
        (void)key; (void)data;
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS::Append(Value& key, const ConstValue& data)
    {
        (void)key; (void)data;
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS::Delete(const ConstValue& key)
    {
        (void)key;
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS::Contains(const ConstValue& key) const
    {
        (void)key;
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS::Length(const ConstValue& key, size_t& len) const
    {
        (void)key; (void)len;
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS::Get(const ConstValue& key, Value& data, size_t offset, size_t len) const
    {
        (void)key; (void)data; (void)offset; (void)len;
        return UNSUPPORTED;
    }

    FawnDS_ConstIterator
    FawnDS::Enumerate() const
    {
        std::cerr << "unsupported Enumerate() called" << std::endl;
        return FawnDS_ConstIterator();
    }

    FawnDS_Iterator
    FawnDS::Enumerate()
    {
        std::cerr << "unsupported Enumerate() called" << std::endl;
        return FawnDS_Iterator();
    }

    FawnDS_ConstIterator
    FawnDS::Find(const ConstValue& key) const
    {
        std::cerr << "unsupported Find() called" << std::endl;
        (void)key;
        return FawnDS_ConstIterator();
    }

    FawnDS_Iterator
    FawnDS::Find(const ConstValue& key)
    {
        std::cerr << "unsupported Find() called" << std::endl;
        (void)key;
        return FawnDS_Iterator();
    }

} // namespace fawn

