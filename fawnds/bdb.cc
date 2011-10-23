/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "bdb.h"
#include "configuration.h"
#include <cassert>
#include <sstream>

#ifdef HAVE_LIBDB

namespace fawn {

    BDB::BDB()
        : dbp_(NULL)
    {
    }

    BDB::~BDB()
    {
        if (dbp_)
            Close();
    }

    FawnDS_Return
    BDB::Create()
    {
        if (dbp_)
            return ERROR;

        // TODO: use DB_ENV to optimize BDB for SSD?

        int ret = db_create(&dbp_, NULL, 0);
        if (ret != 0) {
            fprintf(stderr, "BDB::Create(): Error while creating DB: %s\n", db_strerror(ret));
            return ERROR;
        }

        std::string filename = config_->GetStringValue("child::file") + "_";
        filename += config_->GetStringValue("child::id");

        ret = dbp_->open(dbp_, NULL, filename.c_str(), NULL, DB_BTREE, DB_THREAD | DB_CREATE | DB_TRUNCATE, 0);
        if (ret != 0) {
            fprintf(stderr, "BDB::Create(): Error while creating DB: %s\n", db_strerror(ret));
            dbp_->close(dbp_, 0);
            dbp_ = NULL;
            return ERROR;
        }

        size_ = 0;

        return OK;
    }

    FawnDS_Return
    BDB::Open()
    {
        if (dbp_)
            return ERROR;

        int ret = db_create(&dbp_, NULL, 0);
        if (ret != 0) {
            fprintf(stderr, "BDB::Open(): Error while creating DB: %s\n", db_strerror(ret));
            return ERROR;
        }

        std::string filename = config_->GetStringValue("child::file") + "_";
        filename += config_->GetStringValue("child::id");

        ret = dbp_->open(dbp_, NULL, filename.c_str(), NULL, DB_BTREE, DB_THREAD, 0);
        if (ret != 0) {
            fprintf(stderr, "BDB::Open(): Error while creating DB: %s\n", db_strerror(ret));
            dbp_->close(dbp_, 0);
            dbp_ = NULL;
            return ERROR;
        }

        // TODO: load size_ from file
        size_ = 0;

        return OK;
    }

    FawnDS_Return
    BDB::ConvertTo(FawnDS* new_store) const
    {
        BDB* bdb = dynamic_cast<BDB*>(new_store);
        if (!bdb)
            return UNSUPPORTED;

        bdb->Close();

        std::string src_filename = config_->GetStringValue("child::file") + "_";
        src_filename += config_->GetStringValue("child::id");

        std::string dest_filename = bdb->config_->GetStringValue("child::file") + "_";
        dest_filename += bdb->config_->GetStringValue("child::id");

        if (unlink(dest_filename.c_str())) {
            fprintf(stderr, "BDB::ConvertTo(): cannot unlink the destination file\n");
        }
        if (link(src_filename.c_str(), dest_filename.c_str())) {
            fprintf(stderr, "BDB::ConvertTo(): cannot link the destination file\n");
        }

        if (bdb->Open() != OK)
            return ERROR;

        return OK;
    }

    FawnDS_Return
    BDB::Flush()
    {
        if (!dbp_)
            return ERROR;

        // TODO: implement
        return OK;
    }

    FawnDS_Return
    BDB::Close()
    {
        if (!dbp_)
            return ERROR;

        dbp_->close(dbp_, 0);
        dbp_ = NULL;

        return OK;
    }

    FawnDS_Return
    BDB::Destroy()
    {
        if (dbp_)
            return ERROR;

        DB* dbp;

        int ret = db_create(&dbp, NULL, 0);
        if (ret != 0) {
            fprintf(stderr, "BDB::Destroy(): Error while creating DB: %s\n", db_strerror(ret));
            return ERROR;
        }

        std::string filename = config_->GetStringValue("child::file") + "_";
        filename += config_->GetStringValue("child::id");

        ret = dbp->remove(dbp, filename.c_str(), NULL, 0);
        if (ret != 0) {
            fprintf(stderr, "BDB::Destroy(): Error while removing DB: %s\n", db_strerror(ret));
            dbp->close(dbp, 0);
            return ERROR;
        }

        dbp->close(dbp, 0);
        return OK;
    }

    FawnDS_Return
    BDB::Status(const FawnDS_StatusType& type, Value& status) const
    {
        if (!dbp_)
            return ERROR;

        std::ostringstream oss;
        switch (type) {
        case NUM_DATA:
        case NUM_ACTIVE_DATA:
            oss << size_;
            break;
		case CAPACITY:
            oss << -1;      // unlimited
            break;
        case MEMORY_USE:
            {
                uint32_t gbytes = 0;
                uint32_t bytes = 0;
                int ncache = 0;
                int ret = dbp_->get_cachesize(dbp_, &gbytes, &bytes, &ncache);
                if (ret != 0) {
                    fprintf(stderr, "BDB::Status(): Error while querying DB: %s\n", db_strerror(ret));
                    return ERROR;
                }
                oss << gbytes * (1024 * 1024 * 1024) + bytes;   // BDB uses powers of two
            }
            break;
        case DISK_USE:
            {
                // TODO: check if this work
                DB_BTREE_STAT stat;
                int ret = dbp_->stat(dbp_, NULL, &stat, DB_FAST_STAT);
                if (ret != 0) {
                    fprintf(stderr, "BDB::Status(): Error while querying DB: %s\n", db_strerror(ret));
                    return ERROR;
                }
                oss << stat.bt_pagecnt * stat.bt_pagesize;
            }
            break;
        default:
            return UNSUPPORTED;
        }
        status = NewValue(oss.str());
        return OK;
    }

    FawnDS_Return
    BDB::Put(const ConstValue& key, const ConstValue& data)
    {
        if (!dbp_)
            return ERROR;

        if (key.size() == 0)
            return INVALID_KEY;

        DBT key_v;
        memset(&key_v, 0, sizeof(DBT));
        key_v.data = const_cast<char*>(key.data());
        key_v.size = key.size();

        DBT data_v;
        memset(&data_v, 0, sizeof(DBT));
        data_v.data = const_cast<char*>(data.data());
        data_v.size = data.size();

        int ret = dbp_->put(dbp_, NULL, &key_v, &data_v, DB_NOOVERWRITE);
        if (ret == 0) {
            ++size_;
            return OK;
        }
        else if (ret == DB_KEYEXIST)
            ret = dbp_->put(dbp_, NULL, &key_v, &data_v, 0);

        if (ret != 0) {
            fprintf(stderr, "BDB::Put(): %s\n", db_strerror(ret));
            return ERROR;
        }

        return OK;
    }

    FawnDS_Return
    BDB::Delete(const ConstValue& key)
    {
        if (!dbp_)
            return ERROR;

        if (key.size() == 0)
            return INVALID_KEY;

        DBT key_v;
        memset(&key_v, 0, sizeof(DBT));
        key_v.data = const_cast<char*>(key.data());
        key_v.size = key.size();

        int ret = dbp_->del(dbp_, NULL, &key_v, 0);
        if (ret == 0) {
            --size_;
            return OK;
        }
        else if (ret == DB_NOTFOUND)
            return KEY_NOT_FOUND;
        if (ret != 0) {
            fprintf(stderr, "BDB::Delete(): %s\n", db_strerror(ret));
            return ERROR;
        }

        return OK;
    }

    FawnDS_Return
    BDB::Contains(const ConstValue& key) const
    {
        if (!dbp_)
            return ERROR;

        if (key.size() == 0)
            return INVALID_KEY;

        DBT key_v;
        memset(&key_v, 0, sizeof(DBT));
        key_v.data = const_cast<char*>(key.data());
        key_v.size = key.size();

        int ret = dbp_->exists(dbp_, NULL, &key_v, 0);
        if (ret == 0)
            return OK;
        else if (ret == DB_NOTFOUND)
            return KEY_NOT_FOUND;
        else {
            fprintf(stderr, "BDB::Contains(): %s\n", db_strerror(ret));
            return ERROR;
        }
    }

    FawnDS_Return
    BDB::Length(const ConstValue& key, size_t& len) const
    {
        if (!dbp_)
            return ERROR;

        if (key.size() == 0)
            return INVALID_KEY;

        DBT key_v;
        memset(&key_v, 0, sizeof(DBT));
        key_v.data = const_cast<char*>(key.data());
        key_v.size = key.size();

        DBT data_v;
        memset(&data_v, 0, sizeof(DBT));
        data_v.flags = DB_DBT_USERMEM;

        int ret = dbp_->get(dbp_, NULL, &key_v, &data_v, 0);
        if (ret == 0 || ret == DB_BUFFER_SMALL) {
            len = data_v.size;
            return OK;
        }
        else if (ret == DB_NOTFOUND)
            return KEY_NOT_FOUND;
        else {
            fprintf(stderr, "BDB::Length(): %s\n", db_strerror(ret));
            return ERROR;
        }
    }

    FawnDS_Return
    BDB::Get(const ConstValue& key, Value& data, size_t offset, size_t len) const
    {
        if (!dbp_)
            return ERROR;

        if (key.size() == 0)
            return INVALID_KEY;

        size_t data_len = 0;
        FawnDS_Return ret_len = Length(key, data_len);
        if (ret_len != OK)
            return ret_len;

        if (offset > data_len)
            return END;

        if (offset + len > data_len)
            len = data_len - offset;

        data.resize(len, false);

        DBT key_v;
        memset(&key_v, 0, sizeof(DBT));
        key_v.data = const_cast<char*>(key.data());
        key_v.size = key.size();

        DBT data_v;
        memset(&data_v, 0, sizeof(DBT));
        data_v.data = data.data();
        data_v.ulen = len;
        data_v.flags = DB_DBT_USERMEM;

        int ret = dbp_->get(dbp_, NULL, &key_v, &data_v, 0);
        if (ret != 0) {
            fprintf(stderr, "BDB::Get(): %s\n", db_strerror(ret));
            return ERROR;
        }
        return OK;
    }

    FawnDS_ConstIterator
    BDB::Enumerate() const
    {
        if (!dbp_)
            return FawnDS_ConstIterator();

        IteratorElem* elem = new IteratorElem(this);

        int ret = dbp_->cursor(dbp_, NULL, &elem->cursor, 0);
        if (ret != 0) {
            fprintf(stderr, "BDB::Enumerate(): %s\n", db_strerror(ret));
            return FawnDS_ConstIterator();
        }

        elem->Increment(true);

        return FawnDS_ConstIterator(elem);
    }

    FawnDS_Iterator
    BDB::Enumerate()
    {
        if (!dbp_)
            return FawnDS_Iterator();

        IteratorElem* elem = new IteratorElem(this);

        int ret = dbp_->cursor(dbp_, NULL, &elem->cursor, 0);
        if (ret != 0) {
            fprintf(stderr, "BDB::Enumerate(): %s\n", db_strerror(ret));
            return FawnDS_Iterator();
        }

        elem->Increment(true);

        return FawnDS_Iterator(elem);
    }

    FawnDS_ConstIterator
    BDB::Find(const ConstValue& key) const
    {
        if (!dbp_)
            return FawnDS_ConstIterator();

        if (key.size() == 0)
            return FawnDS_ConstIterator();

        IteratorElem* elem = new IteratorElem(this);

        elem->key = key;

        int ret = dbp_->cursor(dbp_, NULL, &elem->cursor, 0);
        if (ret != 0) {
            fprintf(stderr, "BDB::Find(): %s\n", db_strerror(ret));
            delete elem;
            return FawnDS_ConstIterator();
        }

        elem->Increment(true);

        return FawnDS_ConstIterator(elem);
    }

    FawnDS_Iterator
    BDB::Find(const ConstValue& key)
    {
        if (!dbp_)
            return FawnDS_Iterator();

        if (key.size() == 0)
            return FawnDS_Iterator();

        IteratorElem* elem = new IteratorElem(this);

        elem->key = key;

        int ret = dbp_->cursor(dbp_, NULL, &elem->cursor, 0);
        if (ret != 0) {
            fprintf(stderr, "BDB::Find(): %s\n", db_strerror(ret));
            delete elem;
            return FawnDS_Iterator();
        }

        elem->Increment(true);

        return FawnDS_Iterator(elem);
    }

    BDB::IteratorElem::IteratorElem(const BDB* fawnds)
    {
        this->fawnds = fawnds;
    }

    BDB::IteratorElem::~IteratorElem()
    {
        cursor->close(cursor);
        cursor = NULL;
    }

    FawnDS_IteratorElem*
    BDB::IteratorElem::Clone() const
    {
        IteratorElem* elem = new IteratorElem(static_cast<const BDB*>(fawnds));
        *elem = *this;
        int ret = cursor->dup(cursor, &elem->cursor, DB_POSITION);
        if (ret != 0) {
            fprintf(stderr, "BDB::Clone(): %s\n", db_strerror(ret));
            return NULL;
        }
        return elem;
    }

    void
    BDB::IteratorElem::Next()
    {
        Increment(false);
    }

    void
    BDB::IteratorElem::Increment(bool initial)
    {
        int flags;

        DBT key_v;
        memset(&key_v, 0, sizeof(DBT));
        DBT data_v;
        memset(&data_v, 0, sizeof(DBT));

        if (initial) {
            if (key.size() == 0) {
                flags = DB_FIRST;
                key_v.flags = DB_DBT_USERMEM;
                data_v.flags = DB_DBT_USERMEM;
            }
            else {
                flags = DB_SET;
                key_v.data = key.data();
                key_v.size = key.size();
                data_v.flags = DB_DBT_USERMEM;
            }
        }
        else {
            key_v.flags = DB_DBT_USERMEM;
            data_v.flags = DB_DBT_USERMEM;
            flags = DB_NEXT;
        }

        // obtain the length of key & data
        int ret = cursor->get(cursor, &key_v, &data_v, flags);
        if (ret == 0) {
            // this should not happen because we have key_v.ulen = 0, and there is no zero-length key
            assert(false);
            state = END;
            return;
        }
        else if (ret == DB_NOTFOUND) {
            state = END;
            return;
        }
        else if (ret != DB_BUFFER_SMALL) {
            fprintf(stderr, "BDB::IteratorElem::Increment(): Error while obtaining length: %s\n", db_strerror(ret));
            state = END;
            return;
        }

        //fprintf(stderr, "%d %d\n", key_v.size, data_v.size);

        // retrieve key & data
        key.resize(key_v.size, false);
        key_v.data = key.data();
        key_v.size = key.size();
        key_v.ulen = key_v.size;
        key_v.flags = DB_DBT_USERMEM;

        data.resize(data_v.size, false);
        data_v.data = data.data();
        data_v.size = data.size();
        data_v.ulen = data_v.size;
        key_v.flags = DB_DBT_USERMEM;

        ret = cursor->get(cursor, &key_v, &data_v, flags);
        if (ret != 0) {
            fprintf(stderr, "BDB::IteratorElem::Increment(): Error while reading key and data: %s\n", db_strerror(ret));
            state = END;
            return;
        }

        state = OK;
        key.resize(key_v.size);
        data.resize(data_v.size);
    }

}  // namespace fawn

#endif
