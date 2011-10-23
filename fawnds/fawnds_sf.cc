/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "fawnds_sf.h"
#include "file_io.h"
#include "configuration.h"
#include "fawnds_factory.h"
#include "print.h"
#include "debug.h"
#include "global_limits.h"
#include <algorithm>
#include <map>

#include <cerrno>
#include <cassert>
#include <sstream>

namespace fawn {

/***************************************************/
/******** DB PUBLIC CREATION/OPEN FUNCTIONS ********/
/***************************************************/

// User must specify what kind of backing to use for the
// hash table:  MALLOC_HASHTABLE or MMAP_HASHTABLE.
// When in doubt, use MALLOC_HASHTABLE to avoid any random
// writes to the storage device.

// MMAP_HASHTABLE might be useful in some circumstances.  While
// mmap does flush writes to disk periodically, newer flash
// devices can handle some random writes
// across a *small* address space using block remapping (see
// Polte/Simsa/Gibson WISH2009 paper). One might trade off a bit
// of this performance hit to support more small objects.  This
// technique should be approached only after optimizing the
// hashtable structure per-object memory requirements and
// increasing the number of vnodes (reducing the number of objects
// stored per file).

// As MMAP_HASHTABLE never got used, we removed it.



/***************************************************/
/***************** DB CONSTRUCTOR ******************/
/***************************************************/

FawnDS_SF::FawnDS_SF()
    : header_(NULL), hash_table_(NULL), data_store_(NULL)
{
    pthread_rwlock_init(&fawnds_lock_, NULL);
}

FawnDS_SF::~FawnDS_SF()
{
    if (header_)
        Close();
    pthread_rwlock_destroy(&fawnds_lock_);
}

FawnDS_Return
FawnDS_SF::Create()
{
    DPRINTF(2, "FawnDS_SF::Create(): creating header\n");

    if (config_->ExistsNode("child::key-len") == 0)
        key_len_ = atoi(config_->GetStringValue("child::key-len").c_str());
    else
        key_len_ = 0;

    if (config_->ExistsNode("child::data-len") == 0)
        data_len_ = atoi(config_->GetStringValue("child::data-len").c_str());
    else
        data_len_ = 0;

    header_ = new DbHeader;
    if (header_ == NULL) {
        perror("could not create new header file\n");
        return ERROR;
    }
    // zero out the buffer
    memset(header_, 0, sizeof(DbHeader));

    // populate the database header.
    header_->num_elements = 0;
    header_->num_active_elements = 0;
    //fawnds->header_->max_deleted_ratio = MAX_DELETED_RATIO;
    //fawnds->header_->max_load_factor = MAX_LOAD_FACTOR;

    DPRINTF(2, "FawnDS_SF::Create(): creating hash table\n");

    Configuration* tableConfig = new Configuration(config_, true);
    tableConfig->SetContextNode("child::hashtable");
    tableConfig->SetStringValue("child::id", config_->GetStringValue("child::id"));
    hash_table_ = FawnDS_Factory::New(tableConfig);

    if (hash_table_->Create() != OK) {
        perror("Could not initialize hash_table\n");
        return ERROR;
    }

    DPRINTF(2, "FawnDS_SF::Create(): creating data store\n");

    Configuration* storeConfig = new Configuration(config_, true);
    storeConfig->SetContextNode("child::datastore");
    storeConfig->SetStringValue("child::id", config_->GetStringValue("child::id"));
    if (key_len_ == 0 || data_len_ == 0) {
        storeConfig->SetStringValue("child::data-len", "0");
    }
    else {
        char buf[1024];
        if (key_len_ == 0)
            snprintf(buf, sizeof(buf), "%zu", sizeof(DataHeaderFull) + key_len_ + data_len_);
        else
            snprintf(buf, sizeof(buf), "%zu", sizeof(DataHeaderSimple) + key_len_ + data_len_);
        storeConfig->SetStringValue("child::data-len", buf);
    }
    data_store_ = FawnDS_Factory::New(storeConfig);
    if (data_store_ == NULL) {
        DPRINTF(2, "FawnDS_SF::Create(): could not create data store\n");
        return ERROR;
    }

    if (data_store_->Create() != OK)
        return ERROR;

    DPRINTF(2, "FawnDS_SF::Create(): <result> done\n");

    return OK;
}

FawnDS_Return
FawnDS_SF::Open()
{
    if (config_->ExistsNode("child::key-len") == 0)
        key_len_ = atoi(config_->GetStringValue("child::key-len").c_str());
    else
        key_len_ = 0;

    if (config_->ExistsNode("child::data-len") == 0)
        data_len_ = atoi(config_->GetStringValue("child::data-len").c_str());
    else
        data_len_ = 0;

    DPRINTF(2, "FawnDS_SF::Open(): reading header\n");

    if (ReadHeaderFromFile() != 0) {
        perror("Could not read header from file\n");
        printf("Populating FawnDS from file failed\n");
        return ERROR;
    }

    DPRINTF(2, "FawnDS_SF::Open(): opening hash table\n");

    Configuration* tableConfig = new Configuration(config_, true);
    tableConfig->SetContextNode("child::hashtable");
    tableConfig->SetStringValue("child::id", config_->GetStringValue("child::id"));
    hash_table_ = FawnDS_Factory::New(tableConfig);

    if (hash_table_->Open() != OK) {
        perror("Could not read hash_table from file\n");
        printf("Populating FawnDS from file failed\n");
        return ERROR;
    }

    DPRINTF(2, "FawnDS_SF::Open(): opening data store\n");

    Configuration* storeConfig = new Configuration(config_, true);
    storeConfig->SetContextNode("child::datastore");
    storeConfig->SetStringValue("child::id", config_->GetStringValue("child::id"));
    if (key_len_ != 0 && data_len_ != 0) {
        char buf[1024];
        snprintf(buf, sizeof(buf), "%zu", key_len_ + data_len_);
        storeConfig->SetStringValue("child::data-len", buf);
    }
    data_store_ = FawnDS_Factory::New(storeConfig);

    if (data_store_->Open() != OK)
        return ERROR;

    DPRINTF(2, "FawnDS_SF::Open(): <result> done\n");

    return OK;
}

FawnDS_Return
FawnDS_SF::ConvertTo(FawnDS* new_store) const
{
    pthread_rwlock_rdlock(&fawnds_lock_);

    FawnDS_SF* sf = dynamic_cast<FawnDS_SF*>(new_store);
    if (!sf) {
        pthread_rwlock_unlock(&fawnds_lock_);
        return ERROR;
    }

    if (key_len_ != sf->key_len_) {
        fprintf(stderr, "FawnDS_SF::ConvertTo(): key length mismatch\n");
        pthread_rwlock_unlock(&fawnds_lock_);
        return ERROR;
    }

    if (data_len_ != sf->data_len_) {
        fprintf(stderr, "FawnDS_SF::ConvertTo(): data length mismatch\n");
        pthread_rwlock_unlock(&fawnds_lock_);
        return ERROR;
    }

    if (hash_table_->ConvertTo(sf->hash_table_) != OK) {
        fprintf(stderr, "FawnDS_SF::ConvertTo(): could not convert hash table\n");
        pthread_rwlock_unlock(&fawnds_lock_);
        return ERROR;
    }

    //fprintf(stderr, "FawnDS_SF::ConvertTo(): analyzing hash table changes\n");

    if (sf->config_->ExistsNode("child::keep-datastore-on-conversion") == 0 &&
        atoi(sf->config_->GetStringValue("child::keep-datastore-on-conversion").c_str()) != 0) {
        if (data_store_->ConvertTo(sf->data_store_) != OK) {
            fprintf(stderr, "FawnDS_SF::ConvertTo(): could not convert data store\n");
            pthread_rwlock_unlock(&fawnds_lock_);
            return ERROR;
        }

        sf->header_->num_elements = header_->num_elements;
        sf->header_->num_active_elements = header_->num_active_elements;

        pthread_rwlock_unlock(&fawnds_lock_);
        return OK;
    }

    // side-by-side hash table comparison for data reorganization on data store

    FawnDS_Iterator from_it = hash_table_->Enumerate();
    FawnDS_Iterator to_it = sf->hash_table_->Enumerate();

    std::vector<std::pair<uint32_t, uint32_t> > mapping;
    //std::vector<std::pair<uint32_t, uint32_t> > rev_mapping;

    while (true) {
        if (!from_it.IsEnd() && !to_it.IsEnd()) {
            uint32_t from = from_it->data.as_number<uint32_t>();
            uint32_t to = to_it->data.as_number<uint32_t>();
            //fprintf(stderr, "FawnDS_SF::ConvertTo(): mapping %zu => %zu\n", static_cast<size_t>(from), static_cast<size_t>(to));
            mapping.push_back(std::make_pair(from, to));
            //rev_mapping.push_back(std::make_pair(to, from));
        }
        else if (from_it.IsEnd() && to_it.IsEnd())
            break;
        else {
            // hash table conversion should have failed
            assert(false);
            pthread_rwlock_unlock(&fawnds_lock_);
            return ERROR;
        }
        GlobalLimits::instance().remove_convert_tokens(1);
        ++from_it;
        GlobalLimits::instance().remove_convert_tokens(1);
        ++to_it;
    }
    std::sort(mapping.begin(), mapping.end());
    //std::sort(rev_mapping.begin(), rev_mapping.end());

    // sequential read, random write (hoping OS adds writeback caching)
    std::vector<std::pair<uint32_t, uint32_t> >::const_iterator it;
    Value data;

    sf->header_->num_elements = 0;
    sf->header_->num_active_elements = 0;

    FawnDS_ConstIterator it_from = data_store_->Enumerate();
    for (it = mapping.begin(); it != mapping.end(); ++it) {
        uint32_t from = (*it).first;
        uint32_t to = (*it).second;

        while (!it_from.IsEnd() && it_from->key.as_number<uint32_t>() < from)
            ++it_from;
        assert(!it_from.IsEnd() && it_from->key.as_number<uint32_t>() == from);

        FawnDS_Return ret = sf->data_store_->Put(RefValue(&to), it_from->data);
        if (ret != OK) {
            pthread_rwlock_unlock(&fawnds_lock_);
            return ERROR;
        }
        if (sf->header_->num_elements < to + 1)
            sf->header_->num_elements = to + 1;
        sf->header_->num_active_elements++;
    }

    /*
    // brute-force reorganization; read all data, write all data.
    // TODO: use Nsort for this
    std::map<uint32_t, Value> read_data;
    FawnDS_ConstIterator it_from = data_store_->Enumerate();
    for (it = mapping.begin(); it != mapping.end(); ++it) {
        uint32_t from = (*it).first;
        uint32_t to = (*it).second;

        while (!it_from.IsEnd() && it_from->key.as_number<uint32_t>() < from)
            ++it_from;
        assert(!it_from.IsEnd() && it_from->key.as_number<uint32_t>() == from);

        read_data.insert(std::make_pair(to, NewValue(it_from->data.data(), it_from->data.size())));

        if (sf->header_->num_elements < to + 1)
            sf->header_->num_elements = to + 1;
        sf->header_->num_active_elements++;
    }

    Value dummy_data;
    assert(key_len_ != 0 && data_len_ != 0);
    dummy_data.resize(sizeof(DataHeaderSimple) + key_len_ + data_len_, false);
    memset(dummy_data.data(), 0, sizeof(DataHeaderSimple));
    memset(dummy_data.data() + sizeof(DataHeaderSimple), 0x55, key_len_ + data_len_);

    std::map<uint32_t, Value>::const_iterator it_read_data = read_data.begin();
    Value data_store_key;
    for (uint32_t to = 0; to < sf->header_->num_elements; to++) {
        FawnDS_Return ret;
        if (to < (*it_read_data).first)
            ret = sf->data_store_->Append(data_store_key, dummy_data);
        else if (to == (*it_read_data).first) {
            ret = sf->data_store_->Append(data_store_key, (*it_read_data).second);
            ++it_read_data;
        }
        else
            assert(false);
        if (ret != OK) {
            pthread_rwlock_unlock(&fawnds_lock_);
            return ERROR;
        }
    }
    assert(it_read_data == read_data.end());
    */

    /*
    // removes any holes to avoid a sparse file,
    // which will decrease direct I/O perfromance
    uint32_t last_to = static_cast<uint32_t>(-1);
    assert(last_to + 1 == 0);

    assert(key_len_ != 0 && data_len_ != 0);
    data.resize(sizeof(DataHeaderSimple) + key_len_ + data_len_, false);
    memset(data.data(), 0, sizeof(DataHeaderSimple));
    memset(data.data() + sizeof(DataHeaderSimple), 0x55, key_len_ + data_len_);

    for (it = rev_mapping.begin(); it != rev_mapping.end(); ++it) {
        uint32_t to = (*it).first;
        while (++last_to != to) {
            FawnDS_Return ret = sf->data_store_->Put(RefValue(&last_to), data);
            if (ret != OK) {
                pthread_rwlock_unlock(&fawnds_lock_);
                return ERROR;
            }
        }
    }
    */

    //fprintf(stderr, "FawnDS_SF::ConvertTo(): flushing data store\n");

    FawnDS_Return ret = sf->data_store_->Flush();
    if (ret != OK) {
        pthread_rwlock_unlock(&fawnds_lock_);
        return ERROR;
    }
    pthread_rwlock_unlock(&fawnds_lock_);
    return OK;
}


/***************************************************/
/****************** DB DESTRUCTOR ******************/
/***************************************************/

FawnDS_Return
FawnDS_SF::Flush()
{
    pthread_rwlock_wrlock(&fawnds_lock_);
    bool success = WriteHeaderToFile() == OK && data_store_->Flush() == OK && hash_table_->Flush() == OK;
    pthread_rwlock_unlock(&fawnds_lock_);
    if (!success)
        return ERROR;
    return OK;
}

FawnDS_Return
FawnDS_SF::Close()
{
    if (!header_)
        return ERROR;

    DPRINTF(2, "FawnDS_SF::Close(): closing\n");

    pthread_rwlock_wrlock(&fawnds_lock_);

    delete header_;
    header_ = NULL;
    delete hash_table_;
    hash_table_ = NULL;
    delete data_store_;
    data_store_ = NULL;

    pthread_rwlock_unlock(&fawnds_lock_);

    return OK;
}

FawnDS_Return
FawnDS_SF::Destroy()
{
    if (header_)
        return ERROR;

    string filename = config_->GetStringValue("child::file") + "_";
    filename += config_->GetStringValue("child::id");
    if (unlink(filename.c_str())) {
        //fprintf(stderr, "could not delete file: %s: %s\n", filename.c_str(), strerror(errno));
    }

    Configuration* tableConfig = new Configuration(config_, true);
    tableConfig->SetContextNode("child::hashtable");
    tableConfig->SetStringValue("child::id", config_->GetStringValue("child::id"));
    FawnDS* hash_table = FawnDS_Factory::New(tableConfig);
    FawnDS_Return ret_destroy_hash_table = hash_table->Destroy();
    delete hash_table;

    Configuration* storeConfig = new Configuration(config_, true);
    storeConfig->SetContextNode("child::datastore");
    storeConfig->SetStringValue("child::id", config_->GetStringValue("child::id"));
    FawnDS* data_store = FawnDS_Factory::New(storeConfig);
    FawnDS_Return ret_destroy_data_store = data_store->Destroy();
    delete data_store;

    if (ret_destroy_hash_table == OK && ret_destroy_data_store == OK)
        return OK;
    else
        return ERROR;
}

FawnDS_Return
FawnDS_SF::Status(const FawnDS_StatusType& type, Value& status) const
{
    if (!header_)
        return ERROR;

    std::ostringstream oss;
    switch (type) {
    case NUM_DATA:
        oss << header_->num_elements;
        break;
    case NUM_ACTIVE_DATA:
        oss << header_->num_active_elements;
        break;
    case CAPACITY:
        return hash_table_->Status(type, status);
    case MEMORY_USE:
    case DISK_USE:
        {
            Value status_hash_table;
            Value status_data_store;
            if (hash_table_->Status(type, status_hash_table) != OK)
                return UNSUPPORTED;
            if (data_store_->Status(type, status_data_store) != OK)
                return UNSUPPORTED;
            oss << '[' << status_hash_table.str();
            oss << ',' << status_data_store.str();
            oss << ']';
        }
        break;
    default:
        return UNSUPPORTED;
    }
    status = NewValue(oss.str());
    return OK;
}

/***************************************************/
/****************** DB FUNCTIONS *******************/
/***************************************************/

FawnDS_Return
FawnDS_SF::Put(const ConstValue& key, const ConstValue& data)
{
#ifdef DEBUG
    if (debug_level & 2) {
        DPRINTF(2, "FawnDS_SF::Put(): key=\n");
        print_payload((const u_char*)key.data(), key.size(), 4);
        DPRINTF(2, "FawnDS_SF::Put(): data=\n");
        print_payload((const u_char*)data.data(), data.size(), 4);
    }
#endif

    if (key.size() == 0)
        return INVALID_KEY;

    if (key_len_ != 0 && key_len_ != key.size())
        return INVALID_KEY;

    if (data_len_ != 0 && data_len_ != data.size())
        return INVALID_DATA;

    return InsertEntry(key, data, false);
}

FawnDS_Return
FawnDS_SF::Delete(const ConstValue& key)
{
#ifdef DEBUG
    if (debug_level & 2) {
        DPRINTF(2, "FawnDS_SF::Delete(): key=\n");
        print_payload((const u_char*)key.data(), key.size(), 4);
    }
#endif

    if (key.size() == 0)
        return INVALID_KEY;

    if (key_len_ != 0 && key_len_ != key.size())
        return INVALID_KEY;

    Value empty_v;
    return InsertEntry(key, empty_v, true);
}

FawnDS_Return
FawnDS_SF::InsertEntry(const ConstValue& key, const ConstValue& data, bool isDelete)
{
    assert(!isDelete || (isDelete && data.size() == 0));

    pthread_rwlock_wrlock(&fawnds_lock_);

    bool newObj = true;

    FawnDS_Iterator hash_it = hash_table_->Find(key);
    size_t dhSize;
    if (key_len_ == 0)
        dhSize = sizeof(DataHeaderFull);
    else
        dhSize = sizeof(DataHeaderSimple);

    // Look for an existing entry in the hashtable so that we can update it or invalidate it
    SizedValue<64> read_hdr;
    Value data_store_key;
    while (!hash_it.IsEnd()) {
        data_store_key = hash_it->data;

        if (data_store_->Get(data_store_key, read_hdr, 0, dhSize) != OK ||
            (key_len_ == 0 && read_hdr.as<DataHeaderFull>().key_len != key.size())) {
            DPRINTF(2, "FawnDS_SF::InsertEntry(): key length mismatch; continue\n");
            ++hash_it;
            continue;
        }

        DPRINTF(2, "FawnDS_SF::InsertEntry(): key length match\n");

        SizedValue<64> read_key;
        if (data_store_->Get(data_store_key, read_key, dhSize, key.size()) != OK ||
            key != read_key) {
            DPRINTF(2, "FawnDS_SF::InsertEntry(): key mismatch; continue\n");
            ++hash_it;
            continue;
        }

        DPRINTF(2, "FawnDS_SF::InsertEntry(): key match\n");
        newObj = false;
        break;
    }

    if (hash_it.IsEnd()) {
        DPRINTF(2, "FawnDS_SF::InsertEntry(): key not found in hash table\n");
    }

    if (!newObj) {
        // attempt to delete existing object (OK to fail)
        data_store_->Delete(key);
    }

    Value entry_id;

    // add new entry

    // TODO: can't we use gather output again?
    SizedValue<4096> buf;

    buf.resize(dhSize + key.size() + data.size(), false);
    if (key_len_ == 0) {
        DataHeaderFull dh;
        dh.key_len = key.size();
        dh.type = isDelete ? 2 : 1;
        memcpy(buf.data(), &dh, dhSize);
    }
    else {
        DataHeaderSimple dh;
        dh.type = isDelete ? 2 : 1;
        memcpy(buf.data(), &dh, dhSize);
    }
    memcpy(buf.data() + dhSize, key.data(), key.size());
    memcpy(buf.data() + dhSize + key.size(), data.data(), data.size());

    FawnDS_Return ret_append = data_store_->Append(entry_id, buf);
    if (ret_append != OK) {
        DPRINTF(2, "FawnDS_SF::InsertEntry(): <result> failed to write entry\n");
        pthread_rwlock_unlock(&fawnds_lock_);
        return ret_append;
    }

    DPRINTF(2, "FawnDS_SF::InsertEntry(): inserted entry ID=%llu\n", entry_id.as_number<long long unsigned>());

    // update the hashtable (since the data was successfully written).
    assert(entry_id.as_number<size_t>(-1) != static_cast<size_t>(-1));

    if (hash_it.IsEnd()) {
        FawnDS_Return ret_put = hash_table_->Put(key, entry_id);
        if (ret_put != OK) {
            DPRINTF(2, "FawnDS_SF::InsertEntry(): <result> failed to add to hashtable\n");
            pthread_rwlock_unlock(&fawnds_lock_);
            // TODO: at this point, the log contains data but the hash table doesn't.
            //       will this make inconsistency (with/without index reloading)
            //       i.e. merging process will consistently lose one entry.
            return ret_put;
        }
    }
    else {
        FawnDS_Return ret_replace = hash_it->Replace(entry_id);
        if (ret_replace != OK) {
            DPRINTF(2, "FawnDS_SF::InsertEntry(): <result> failed to update hashtable\n");
            pthread_rwlock_unlock(&fawnds_lock_);
            // TODO: this must be rare case (maybe a bug), but this also has potential inconsistency issues
            return ret_replace;
        }
    }

    // Update number of elements if Insert added a new object
    if (!isDelete) {
        if (newObj)
            header_->num_active_elements++;
    }
    else {
        // count a delete entry as an active element
        /*
        if (!newObj) {
            header_->num_active_elements--;
        }
        */
    }
    header_->num_elements++;

    DPRINTF(2, "FawnDS_SF::InsertEntry(): <result> updated hashtable\n");

    pthread_rwlock_unlock(&fawnds_lock_);

    if (!isDelete)
        return OK;
    else {
        if (!newObj)
            return OK;
        else
            return KEY_NOT_FOUND;
    }
}

FawnDS_Return
FawnDS_SF::Contains(const ConstValue& key) const
{
    if (key.size() == 0)
        return INVALID_KEY;

    if (key_len_ != 0 && key_len_ != key.size())
        return INVALID_KEY;

    // TODO: fix this naive implementation
    size_t len = 0;
    FawnDS_Return ret = Length(key, len);
    if (ret == OK) {
        return OK;
    }
    else
        return KEY_NOT_FOUND;
}

FawnDS_Return
FawnDS_SF::Length(const ConstValue& key, size_t& len) const
{
#ifdef DEBUG
    if (debug_level & 2) {
        DPRINTF(2, "FawnDS_SF::Length(): key=\n");
        print_payload((const u_char*)key.data(), key.size(), 4);
    }
#endif

    if (key.size() == 0)
        return INVALID_KEY;

    if (key_len_ != 0 && key_len_ != key.size())
        return INVALID_KEY;

    // TODO: implement this in a better way
    SizedValue<4096> data;
    FawnDS_Return ret_get = Get(key, data);

    len = data.size();
#ifdef DEBUG
    if (debug_level & 2)
        DPRINTF(2, "FawnDS_SF::Length(): <result> len=%zu\n", len);
#endif
    return ret_get;
}

FawnDS_Return
FawnDS_SF::Get(const ConstValue& key, Value& data, size_t offset, size_t len) const
{
    if (key.size() == 0)
        return INVALID_KEY;

    if (key_len_ != 0 && key_len_ != key.size())
        return INVALID_KEY;

#ifdef DEBUG
    if (debug_level & 2) {
        DPRINTF(2, "FawnDS_SF::Get(): key=\n");
        print_payload((const u_char*)key.data(), key.size(), 4);
    }
#endif

    pthread_rwlock_rdlock(&fawnds_lock_);

    bool newObj = true;

    FawnDS_Iterator hash_it = hash_table_->Find(key);
    size_t dhSize;
    if (key_len_ == 0)
        dhSize = sizeof(DataHeaderFull);
    else
        dhSize = sizeof(DataHeaderSimple);

    // Look for an existing entry in the hashtable
    // TODO: this code seems to almost redundant to InsertEntry(); any way to simplify?
    Value data_store_key;
    SizedValue<64> read_hdr;
    while (!hash_it.IsEnd()) {
        data_store_key = hash_it->data;

        if (data_store_->Get(data_store_key, read_hdr, 0, dhSize) != OK ||
            (key_len_ == 0 && read_hdr.as<DataHeaderFull>().key_len != key.size())) {
            DPRINTF(2, "FawnDS_SF::Get(): key length mismatch; continue\n");
            ++hash_it;
            continue;
        }

        DPRINTF(2, "FawnDS_SF::Get(): key length match\n");

        SizedValue<64> read_key;
        if (data_store_->Get(data_store_key, read_key, dhSize, key.size()) != OK ||
            key != read_key) {
            DPRINTF(2, "FawnDS_SF::Get(): key mismatch; continue\n");
            ++hash_it;
            continue;
        }

        DPRINTF(2, "FawnDS_SF::Get(): key match\n");
        newObj = false;
        break;
    }

    if (newObj) {
        DPRINTF(2, "FawnDS_SF::Get(): <result> cannot find key\n");
        pthread_rwlock_unlock(&fawnds_lock_);
        return KEY_NOT_FOUND;
    }

    size_t total_len = 0;
    FawnDS_Return ret_len = data_store_->Length(data_store_key, total_len);
    if (ret_len != OK) {
        pthread_rwlock_unlock(&fawnds_lock_);
        return ret_len;
    }

    uint8_t type;
    if (key_len_ == 0)
        type = read_hdr.as<DataHeaderFull>().type;
    else
        type = read_hdr.as<DataHeaderSimple>().type;

    if (type == 0) {
        // this should not happend -- an unused entry is referenced by the hash table
        assert(false);
        return ERROR;
    }
    else if (type == 2) {
        DPRINTF(2, "FawnDS_SF::Get(): <result> deleted key\n");
        pthread_rwlock_unlock(&fawnds_lock_);
        return KEY_DELETED;
    }

    size_t data_len = total_len - dhSize - key.size();

    if (offset > data_len) {
        pthread_rwlock_unlock(&fawnds_lock_);
        data.resize(0);
        return OK;
    }

    if (offset + len > data_len)
        len = data_len - offset;

    FawnDS_Return ret_data = data_store_->Get(data_store_key, data, dhSize + key.size() + offset, len);

#ifdef DEBUG
    if (debug_level & 2) {
        DPRINTF(2, "FawnDS_SF::Get(): <result> data=\n");
        print_payload((const u_char*)data.data(), data.size(), 4);
    }
#endif

    pthread_rwlock_unlock(&fawnds_lock_);
    return ret_data;
}

FawnDS_ConstIterator
FawnDS_SF::Enumerate() const
{
    IteratorElem* elem = new IteratorElem(this);
    elem->data_store_it = data_store_->Enumerate();
    elem->Increment(true);
    return FawnDS_ConstIterator(elem);
}

FawnDS_Iterator
FawnDS_SF::Enumerate()
{
    IteratorElem* elem = new IteratorElem(this);
    elem->data_store_it = data_store_->Enumerate();
    elem->Increment(true);
    return FawnDS_Iterator(elem);
}

FawnDS_SF::IteratorElem::IteratorElem(const FawnDS_SF* fawnds)
{
    this->fawnds = fawnds;
    pthread_rwlock_rdlock(&static_cast<const FawnDS_SF*>(fawnds)->fawnds_lock_);
}

FawnDS_SF::IteratorElem::~IteratorElem()
{
    pthread_rwlock_unlock(&static_cast<const FawnDS_SF*>(fawnds)->fawnds_lock_);
}

FawnDS_IteratorElem*
FawnDS_SF::IteratorElem::Clone() const
{
    IteratorElem* elem = new IteratorElem(static_cast<const FawnDS_SF*>(fawnds));
    *elem = *this;
    return elem;
}

void
FawnDS_SF::IteratorElem::Next()
{
    Increment(false);
}

void
FawnDS_SF::IteratorElem::Increment(bool initial)
{
    while (true) {
        if (initial)
            initial = false;
        else
            ++data_store_it;

        const FawnDS_SF* fawnds_sf = static_cast<const FawnDS_SF*>(fawnds);

        if (data_store_it.IsOK()) {
            Value& data_store_data = data_store_it->data;

            size_t dhSize;
            size_t key_len;
            uint8_t type;
            if (fawnds_sf->key_len_ == 0) {
                dhSize = sizeof(DataHeaderFull);
                key_len = data_store_data.as<DataHeaderFull>().key_len;
                type = data_store_data.as<DataHeaderFull>().type;
            }
            else {
                dhSize = sizeof(DataHeaderSimple);
                key_len = fawnds_sf->key_len_;
                type = data_store_data.as<DataHeaderSimple>().type;
            }

            if (type == 0) {
                // skip unused space
                continue;
            }
            else if (type == 1)
                state = OK;
            else if (type == 2)
                state = KEY_DELETED;
            else {
                // corrupted data?
                state = ERROR;
                assert(false);
            }

            key = NewValue(data_store_data.data() + dhSize, key_len);
            data = NewValue(data_store_data.data() + dhSize + key_len, data_store_data.size() - dhSize - key_len);
            break;
        }
        else if (data_store_it.IsEnd()) {
            state = END;
            break;
        }
        else {
            state = ERROR;
            break;
        }
    }
}

/***************************************************/
/************** DB Utility Functions ***************/
/***************************************************/

FawnDS_Return
FawnDS_SF::WriteHeaderToFile() const {
    uint64_t length = sizeof(struct DbHeader);
    uint64_t offset = 0;

    string filename = config_->GetStringValue("child::file") + "_";
    filename += config_->GetStringValue("child::id");
    int fd;
    if ((fd = open(filename.c_str(), O_RDWR|O_CREAT|O_NOATIME, 0666)) == -1) {
        perror("Could not open header file");
        return ERROR;
    }

    if (ftruncate(fd, (off_t)length) == -1) {
        fprintf(stderr, "Could not extend header file to %"PRIu64" bytes: \
                %s\n", length, strerror(errno));
    }
    lseek(fd, 0, SEEK_SET);

    if (fill_file_with_zeros(fd, length) < 0) {
        fprintf(stderr, "Could not zero out header file.\n");
        close(fd);
        return ERROR;
    }

    // write the header for the hashtable
    if ((uint64_t)pwrite64(fd, header_, length, offset) != length) {
        fprintf(stderr, "Could not write malloc'd dbheader at position %d: \
                    %s\n", 0, strerror(errno));
        close(fd);
        return ERROR;
    }

    fdatasync(fd);
    close(fd);
    return OK;
}

// This assumes that the file was closed properly.
FawnDS_Return
FawnDS_SF::ReadHeaderFromFile()
{
    cout << "Reading header from file..." << endl;
    header_ = (struct DbHeader*)malloc(sizeof(struct DbHeader));
    if (header_ == NULL) {
        perror("could not malloc header file\n");
        return ERROR;
    }

    string filename = config_->GetStringValue("child::file") + "_";
    filename += config_->GetStringValue("child::id");
    int fd;
    if ((fd = open(filename.c_str(), O_RDWR|O_CREAT|O_NOATIME, 0666)) == -1)
    {
        perror("Could not open header file");
        return ERROR;
    }
    uint64_t length = sizeof(struct DbHeader);
    uint64_t offset = 0;
    if ((uint64_t)pread64(fd, header_, length, offset) != length) {
        fprintf(stderr, "Could not read header for data at position \
                    %"PRIu64": %s\n", offset, strerror(errno));
        return ERROR;
    }
    return OK;
}

}  // namespace fawn

