/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "fawnds_partition.h"
#include "fawnds_factory.h"
#include "debug.h"
#include "bit_access.hpp"
#include <sstream>

namespace fawn {

    FawnDS_Partition::FawnDS_Partition()
    {
    }

    FawnDS_Partition::~FawnDS_Partition()
    {
        Close();
    }

    FawnDS_Return
    FawnDS_Partition::Create()
    {
        if (stores_.size())
            return ERROR;

        alloc_stores();

        for (size_t i = 0; i < partitions_; i++) {
            if (stores_[i]->Create() != OK) {
                return ERROR;
            }
        }

        return OK;
    }

    FawnDS_Return
    FawnDS_Partition::Open()
    {
        if (stores_.size())
            return ERROR;

        alloc_stores();

        for (size_t i = 0; i < partitions_; i++) {
            if (stores_[i]->Open() != OK) {
                return ERROR;
            }
        }

        return OK;
    }

    FawnDS_Return
    FawnDS_Partition::Flush()
    {
        if (!stores_.size())
            return ERROR;

        for (std::vector<FawnDS*>::iterator it = stores_.begin(); it != stores_.end(); ++it) {
            FawnDS_Return ret = (*it)->Flush();
            if (ret != OK)
                return ret;
        }
        return OK;
    }

    FawnDS_Return
    FawnDS_Partition::Close()
    {
        if (!stores_.size())
            return ERROR;

        for (std::vector<FawnDS*>::iterator it = stores_.begin(); it != stores_.end(); ++it) {
            if (*it) {
                FawnDS_Return ret = (*it)->Close();
                if (ret != OK)
                    return ret;
                delete *it;
                *it = NULL;
            }
        }
        stores_.clear();
        return OK;
    }

    FawnDS_Return
    FawnDS_Partition::Destroy()
    {
        for (size_t i = 0; i < partitions_; i++) {
            Configuration* storeConfig = new Configuration(config_, true);
            storeConfig->SetContextNode("child::store");

            char buf[1024];
            snprintf(buf, sizeof(buf), "%s_%zu", config_->GetStringValue("child::id").c_str(), i);
            storeConfig->SetStringValue("child::id", buf);

            FawnDS* store = FawnDS_Factory::New(storeConfig);
            if (!store) {
                delete storeConfig;
                return ERROR;
            }
            store->Destroy();
            delete store;
        }

        return OK;
    }

	FawnDS_Return
	FawnDS_Partition::Status(const FawnDS_StatusType& type, Value& status) const
	{
        std::ostringstream oss;
        switch (type) {
        case NUM_DATA:
        case NUM_ACTIVE_DATA:
		case CAPACITY:
        case MEMORY_USE:
        case DISK_USE:
            {
                bool first = true;
                Value status_part;
                oss << '[';
                for (std::vector<FawnDS*>::const_iterator it = stores_.begin(); it != stores_.end(); ++it) {
                    if (first)
                        first = false;
                    else
                        oss << ',';
                    FawnDS_Return ret = (*it)->Status(type, status_part);
                    if (ret != OK)
                        return UNSUPPORTED;
                    oss << status_part.str();
                }
                oss << ']';
            }
            break;
        default:
            return UNSUPPORTED;
        }
        status = NewValue(oss.str());
        return OK;
    }

    FawnDS_Return
    FawnDS_Partition::Put(const ConstValue& key, const ConstValue& data)
    {
        if (!stores_.size())
            return ERROR;

        size_t p = get_partition(key);
        return stores_[p]->Put(key, data);
    }

    FawnDS_Return
    FawnDS_Partition::Append(Value& key, const ConstValue& data)
    {
        if (!stores_.size())
            return ERROR;

        size_t p = get_partition(key);
        return stores_[p]->Append(key, data);
    }

    FawnDS_Return
    FawnDS_Partition::Delete(const ConstValue& key)
    {
        if (!stores_.size())
            return ERROR;

        size_t p = get_partition(key);
        return stores_[p]->Delete(key);
    }

    FawnDS_Return
    FawnDS_Partition::Contains(const ConstValue& key) const
    {
        if (!stores_.size())
            return ERROR;

        size_t p = get_partition(key);
        return stores_[p]->Contains(key);
    }

    FawnDS_Return
    FawnDS_Partition::Length(const ConstValue& key, size_t& len) const
    {
        if (!stores_.size())
            return ERROR;

        size_t p = get_partition(key);
        return stores_[p]->Length(key, len);
    }

    FawnDS_Return
    FawnDS_Partition::Get(const ConstValue& key, Value& data, size_t offset, size_t len) const
    {
        if (!stores_.size())
            return ERROR;

        size_t p = get_partition(key);
        return stores_[p]->Get(key, data, offset, len);
    }

    FawnDS_ConstIterator
    FawnDS_Partition::Enumerate() const
    {
        IteratorElem* elem = new IteratorElem(this);
        elem->next_store = 0;
        elem->Next();
        return FawnDS_ConstIterator(elem);
    }

    FawnDS_Iterator
    FawnDS_Partition::Enumerate()
    {
        IteratorElem* elem = new IteratorElem(this);
        elem->next_store = 0;
        elem->Next();
        return FawnDS_Iterator(elem);
    }

    FawnDS_ConstIterator
    FawnDS_Partition::Find(const ConstValue& key) const
    {
        if (!stores_.size())
            return FawnDS_ConstIterator();

        size_t p = get_partition(key);
        return stores_[p]->Find(key);
    }

    FawnDS_Iterator
    FawnDS_Partition::Find(const ConstValue& key)
    {
        if (!stores_.size())
            return FawnDS_Iterator();

        size_t p = get_partition(key);
        return stores_[p]->Find(key);
    }

    FawnDS_Return
    FawnDS_Partition::alloc_stores()
    {
        skip_bits_ = atoi(config_->GetStringValue("child::skip_bits").c_str());

        partitions_ = atoi(config_->GetStringValue("child::partitions").c_str());
        partition_bits_ = 0;
        while ((1u << partition_bits_) < partitions_)
            partition_bits_++;

        if (partitions_ == 0) {
            DPRINTF(2, "FawnDS_Partition::alloc_stores(): non-zero partitions required\n");
            return ERROR;
        }

        if ((1u << partition_bits_) != partitions_) {
            DPRINTF(2, "FawnDS_Partition::alloc_stores(): # of partitions should be power of 2\n");
            return ERROR;
        }

        for (size_t i = 0; i < partitions_; i++) {
            Configuration* storeConfig = new Configuration(config_, true);
            storeConfig->SetContextNode("child::store");

            char buf[1024];
            snprintf(buf, sizeof(buf), "%s_%zu", config_->GetStringValue("child::id").c_str(), i);
            storeConfig->SetStringValue("child::id", buf);

            FawnDS* store = FawnDS_Factory::New(storeConfig);
            if (!store) {
                delete storeConfig;
                return ERROR;
            }
            stores_.push_back(store);
        }
        return OK;
    }

    size_t
    FawnDS_Partition::get_partition(const ConstValue& key) const
    {
        std::size_t bits = 0;
        std::size_t index = 0;
        while (bits < partition_bits_)
		{
            index <<= 1;
            if ((skip_bits_ + bits) / 8 > key.size())
			{
				// too short key
				assert(false);
                break;
            }
            if (cindex::bit_access::get(reinterpret_cast<const uint8_t*>(key.data()), skip_bits_ + bits))
                index |= 1;
            bits++;
        }

        return index;
    }

    FawnDS_Partition::IteratorElem::IteratorElem(const FawnDS_Partition* fawnds)
    {
        this->fawnds = fawnds;
    }

    FawnDS_Partition::IteratorElem::~IteratorElem()
    {
    }

    FawnDS_IteratorElem*
    FawnDS_Partition::IteratorElem::Clone() const
    {
        IteratorElem* elem = new IteratorElem(static_cast<const FawnDS_Partition*>(fawnds));
        *elem = *this;
        return elem;
    }

    void
    FawnDS_Partition::IteratorElem::Next()
    {
        const FawnDS_Partition* fawnds_part = static_cast<const FawnDS_Partition*>(fawnds);

        if (!store_it.IsEnd())
            ++store_it;

        while (store_it.IsEnd()) {
            if (next_store >= fawnds_part->stores_.size()) {
                state = END;
                return;
            }
            store_it = fawnds_part->stores_[next_store++]->Enumerate();
        }

        state = store_it->state;
        key = store_it->key;
        data = store_it->data;
    }

} // namespace fawn

