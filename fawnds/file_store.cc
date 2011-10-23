/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "file_store.h"
#include "file_io.h"
#include "configuration.h"
#include "debug.h"
#include "print.h"

#include <cstdio>
#include <cerrno>
#include <cstdlib>
#include <sstream>

#include <cassert>

namespace fawn {

    FileStore::FileStore()
    {
        next_append_id_ = end_id_ = 0;
        next_sync_ = 0;

        int_initialize();
    }

    FileStore::~FileStore()
    {
        if (int_is_open())
            int_close();

        int_terminate();
    }

    FawnDS_Return
    FileStore::Create()
    {
        std::string filename = config_->GetStringValue("child::file") + "_";
        filename += config_->GetStringValue("child::id");

        if (config_->ExistsNode("child::data-len") == 0)
            data_len_ = atoi(config_->GetStringValue("child::data-len").c_str());
        else
            data_len_ = 0;

        if (config_->ExistsNode("child::use-buffered-io-only") == 0)
            use_buffered_io_only_ = atoi(config_->GetStringValue("child::use-buffered-io-only").c_str()) != 0;
        else
            use_buffered_io_only_ = false;

        DPRINTF(2, "FileStore::Create(): creating file: %s\n", filename.c_str());

        if (!int_open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_NOATIME, 0666))
            return ERROR;

        next_append_id_ = end_id_ = 0;
        next_sync_ = 1048576 * (1 << (rand() % 4));

        DPRINTF(2, "FileStore::Create(): created file: %s\n", filename.c_str());

        return OK;
    }

    FawnDS_Return
    FileStore::Open()
    {
        std::string filename = config_->GetStringValue("child::file") + "_";
        filename += config_->GetStringValue("child::id");

        if (config_->ExistsNode("child::data-len") == 0)
            data_len_ = atoi(config_->GetStringValue("child::data-len").c_str());
        else
            data_len_ = 0;

        if (config_->ExistsNode("child::use-buffered-io-only") == 0)
            use_buffered_io_only_ = atoi(config_->GetStringValue("child::use-buffered-io-only").c_str()) != 0;
        else
            use_buffered_io_only_ = false;

        DPRINTF(2, "FileStore::Open(): opening file: %s\n", filename.c_str());

        if (!int_open(filename.c_str(), O_RDWR | O_NOATIME, 0666))
            return ERROR;

        // TODO: move this functionality to the I/O layer
        next_append_id_ = end_id_ = lseek(fd_buffered_sequential_, 0, SEEK_END);  // TODO: handling truncated files
        next_sync_ = 1048576 * (1 << (rand() % 4));

        DPRINTF(2, "FileStore::Open(): opened file: %s\n", filename.c_str());
        DPRINTF(2, "FileStore::Open(): next key=%llu\n", static_cast<long long unsigned>(next_append_id_));

        return OK;
    }

    FawnDS_Return
    FileStore::ConvertTo(FawnDS* new_store) const
    {
        FileStore* file_store = dynamic_cast<FileStore*>(new_store);
        if (!file_store)
            return UNSUPPORTED;

        if (data_len_ != file_store->data_len_) {
            fprintf(stderr, "FileStore::ConvertTo(): data length mismatch\n");
            return ERROR;
        }

        if (!file_store->int_close())
            assert(false);

        std::string src_filename = config_->GetStringValue("child::file") + "_";
        src_filename += config_->GetStringValue("child::id");

        std::string dest_filename = file_store->config_->GetStringValue("child::file") + "_";
        dest_filename += file_store->config_->GetStringValue("child::id");

        if (!int_unlink(dest_filename.c_str())) {
            fprintf(stderr, "FileStore::ConvertTo(): cannot unlink the destination file\n");
        }
        // TODO: wrap link() with int_link()
        if (link(src_filename.c_str(), dest_filename.c_str())) {
            fprintf(stderr, "FileStore::ConvertTo(): cannot link the destination file\n");
        }

        if (!file_store->int_open(dest_filename.c_str(), O_RDWR | O_NOATIME, 0666))
            assert(false);

        // TODO: concurrent operations

        file_store->next_append_id_ = next_append_id_;
        file_store->end_id_ = end_id_;
        file_store->next_sync_ = next_sync_;

        return OK;
    }

    FawnDS_Return
    FileStore::Flush()
    {
        if (!int_is_open())
            return OK;

        // TODO: do write lock to ensure all logs have been written
        //       just should the upper store handle concurrent accesses?
        int_sync(true);
        return OK;
    }

    FawnDS_Return
    FileStore::Close()
    {
        if (!int_is_open())
            return OK;

        Flush();

        if (int_close() == -1) {
            fprintf(stderr, "FileStore::Close(): Could not close file properly\n");
            return ERROR;
        }

        DPRINTF(2, "FileStore::Close(): closed file\n");

        return OK;
    }

    FawnDS_Return
    FileStore::Destroy()
    {
        std::string filename = config_->GetStringValue("child::file") + "_";
        filename += config_->GetStringValue("child::id");

        if (!int_unlink(filename.c_str())) {
            fprintf(stderr, "FileStore::Destroy(): could not delete file\n");
            return ERROR;
        }

        return OK;
    }

    FawnDS_Return
    FileStore::Status(const FawnDS_StatusType& type, Value& status) const
    {
        std::ostringstream oss;
        switch (type) {
        case CAPACITY:
            oss << -1;      // unlimited
            break;
        case MEMORY_USE:
            oss << 0;
            break;
        case DISK_USE:
            if (data_len_ == 0)
                oss << end_id_;
            else
                oss << end_id_ * data_len_;
            break;
        default:
            return UNSUPPORTED;
        }
        status = NewValue(oss.str());
        return OK;
    }

    FawnDS_Return
    FileStore::Put(const ConstValue& key, const ConstValue& data)
    {
#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "FileStore::Put(): key=\n");
            print_payload((const u_char*)key.data(), key.size(), 4);
            DPRINTF(2, "FileStore::Put(): data=\n");
            print_payload((const u_char*)data.data(), data.size(), 4);
        }
#endif

        off_t id = key.as_number<off_t>(-1);
        if (id == -1)
            return INVALID_KEY;

        off_t target = id;
        if (data_len_ != 0)
            target *= data_len_;

        entry_length_t entry_len = data.size();
        size_t total_len = entry_len;
        if (data_len_ == 0)
            total_len += sizeof(entry_length_t);

        bool wrote;
        if (data_len_ == 0) {
            struct iovec iov[2];
            iov[0].iov_base = &entry_len;
            iov[0].iov_len = sizeof(entry_length_t);
            iov[1].iov_base = const_cast<char*>(data.data());
            iov[1].iov_len = data.size();
            wrote = int_pwritev(iov, 2, target);
        }
        else {
            struct iovec iov[1];
            iov[0].iov_base = const_cast<char*>(data.data());
            iov[0].iov_len = data.size();
            wrote = int_pwritev(iov, 1, target);
        }

        if (!wrote) {
            fprintf(stderr, "failed to write at file offset %llu: %s\n", static_cast<long long unsigned>(target), strerror(errno));
            return ERROR;
        }

        // TODO: update correctly under concurrent writes
        if (data_len_ == 0) {
            if (end_id_ < static_cast<off_t>(id + total_len))
                end_id_ = static_cast<off_t>(id + total_len);
        }
        else {
            if (end_id_ < id + 1)
                end_id_ = id + 1;
        }

        DPRINTF(2, "FileStore::Put(): <result> data written at file offset %llu\n", static_cast<long long unsigned>(target));
        return OK;
    }

    FawnDS_Return
    FileStore::Append(Value& key, const ConstValue& data)
    {
#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "FileStore::Append(): data=\n");
            print_payload((const u_char*)data.data(), data.size(), 4);
        }
#endif

        if (data_len_ != 0 && data_len_ != data.size())
            return INVALID_DATA;

        off_t key_val;

        size_t append_len;
        if (data_len_ == 0) {
            append_len = sizeof(entry_length_t) + data.size();
            key_val = (next_append_id_ += append_len) - append_len;   // atomically add a new record length, store a new value
        }
        else {
            if (data_len_ != data.size())
                return INVALID_DATA;
            append_len = data.size();
            key_val = ++next_append_id_ - 1;
        }

        key = NewValue(&key_val);

        FawnDS_Return ret = Put(key, data);
        if (ret != OK) {
            DPRINTF(2, "FileStore::Append(): <result> failed\n");
            return ret;
        }

        // regularly clean up OS dirty buffers
        if ((next_sync_ -= append_len) < 0) {
            next_sync_ = 1048576 * (1 << (rand() % 4));
            int_sync(false);
        }

        DPRINTF(2, "FileStore::Append(): <result> key=%zu\n", target);

        return OK;
    }

    FawnDS_Return
    FileStore::Contains(const ConstValue& key) const
    {
        if (key.as_number<off_t>(-1) >= end_id_)
            return KEY_NOT_FOUND;
        else
            return OK;
    }

    FawnDS_Return
    FileStore::Length(const ConstValue& key, size_t& len) const
    {
        return length(key, len, false);
    }

    FawnDS_Return
    FileStore::length(const ConstValue& key, size_t& len, bool readahead) const
    {
        off_t id = key.as_number<off_t>(-1);

        if (key.as_number<off_t>(-1) >= end_id_)
            return KEY_NOT_FOUND;

#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "FileStore::Length(): key=%llu\n", static_cast<long long unsigned>(id));
        }
#endif

        if (data_len_ == 0) {
            SizedValue<sizeof(entry_length_t)> buf;
            size_t count = sizeof(entry_length_t);
            buf.resize(count, false);
            if (!int_pread(buf.data(), count, id, readahead))
                return ERROR;
            buf.resize(count);
            if (buf.size() != sizeof(entry_length_t))
                return ERROR;
            len = buf.as<entry_length_t>();
        }
        else
            len = data_len_;

#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "FileStore::Length(): <result> length=%zu\n", len);
        }
#endif

        return OK;
    }

    FawnDS_Return
    FileStore::Get(const ConstValue& key, Value& data, size_t offset, size_t len) const
    {
        return get(key, data, offset, len, false);
    }

    FawnDS_Return
    FileStore::get(const ConstValue& key, Value& data, size_t offset, size_t len, bool readahead) const
    {
        off_t id = key.as_number<off_t>(-1);

        data.resize(0);

#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "FileStore::Get(): key=%llu\n", static_cast<long long unsigned>(id));
        }
#endif

        if (id >= end_id_)
            return END;

        size_t data_len = 0;
        FawnDS_Return ret_len = length(key, data_len, readahead);
        if (ret_len != OK)
            return ret_len;

        if (offset > data_len)
            return END;

        if (offset + len > data_len)
            len = data_len - offset;

        ssize_t read_len;

        if (data_len_ == 0) {
            size_t count = len;
            data.resize(count, false);
            if (!int_pread(data.data(), count, id + sizeof(entry_length_t) + offset, readahead))
                return ERROR;
            data.resize(count);
        }
        else {
            // try to read the entire entry to reduce I/O for the subseqeunt request
            size_t count = data_len_ - offset;
            data.resize(count, false);
            if (!int_pread(data.data(), count, id * data_len_ + offset, readahead))
                return ERROR;
            data.resize(count);
            if (data.size() > len)
                data.resize(len);
        }

#ifdef DEBUG
        if (debug_level & 2) {
            DPRINTF(2, "FileStore::Get(): <result> data=\n");
            print_payload((const u_char*)data.data(), data.size(), 4);
        }
#endif

        return OK;
    }

    FawnDS_ConstIterator
    FileStore::Enumerate() const
    {
        IteratorElem* elem = new IteratorElem();
        elem->fawnds = this;
        elem->next_id = 0;
        elem->Next();
        return FawnDS_ConstIterator(elem);
    }

    FawnDS_Iterator
    FileStore::Enumerate()
    {
        IteratorElem* elem = new IteratorElem();
        elem->fawnds = this;
        elem->next_id = 0;
        elem->Next();
        return FawnDS_Iterator(elem);
    }

    FawnDS_ConstIterator
    FileStore::Find(const ConstValue& key) const
    {
        IteratorElem* elem = new IteratorElem();
        elem->fawnds = this;
        elem->next_id = key.as_number<off_t>(-1);
        elem->Next();
        return FawnDS_ConstIterator(elem);
    }

    FawnDS_Iterator
    FileStore::Find(const ConstValue& key)
    {
        IteratorElem* elem = new IteratorElem();
        elem->fawnds = this;
        elem->next_id = key.as_number<off_t>(-1);
        elem->Next();
        return FawnDS_Iterator(elem);
    }

    /*
    int
    FileStore::disable_readahead()
    {
        int rc;
#ifdef __APPLE__
        int zero = 0;
        if ((rc = fcntl(fd_, F_RDAHEAD, &zero)) < 0) {
            perror("couldn't fcntl F_RDAHEAD");
        }
#else
        if ((rc = posix_fadvise(fd_, 0, 0, POSIX_FADV_RANDOM)) < 0) {
            perror("couldn't posix_fadvise random");
        }
#endif
        return rc;
    }
    */

    FawnDS_IteratorElem*
    FileStore::IteratorElem::Clone() const
    {
        IteratorElem* elem = new IteratorElem();
        *elem = *this;
        return elem;
    }

    void
    FileStore::IteratorElem::Next()
    {
        const FileStore* file_store = static_cast<const FileStore*>(fawnds);

        key = NewValue(&next_id);
        state = file_store->get(key, data, 0, static_cast<size_t>(-1), true);

        if (state == OK || state == KEY_DELETED) {
            if (file_store->data_len_ == 0)
                next_id += sizeof(entry_length_t) + data.size();
            else
                next_id++;
        }
        else
            state = END;
    }

    void
    FileStore::int_initialize()
    {
        fd_buffered_sequential_ = -1;
        fd_buffered_random_ = -1;
        fd_direct_random_ = -1;
        memset(cache_, 0, sizeof(cache_entry) * cache_size_);
        cache_hit_ = 0;
        cache_miss_ = 0;
    }

    void
    FileStore::int_terminate()
    {
        if (int_is_open()) {
            assert(false);
            int_close();
        }
        //fprintf(stderr, "FileStore::int_terminate(): cache hit = %zu\n", static_cast<size_t>(cache_hit_));
        //fprintf(stderr, "FileStore::int_terminate(): cache miss = %zu\n", static_cast<size_t>(cache_miss_));
    }

    bool
    FileStore::int_open(const char* pathname, int flags, mode_t mode)
    {
        if (int_is_open())
            return false;

        fd_buffered_sequential_ = open(pathname, flags, mode);
        if (fd_buffered_sequential_ == -1) {
            fprintf(stderr, "FileStore::int_open(): cannot open file: %s: %s\n", pathname, strerror(errno));
            return false;
        }

        fd_buffered_random_ = open(pathname, flags & ~(O_CREAT | ~O_TRUNC | O_RDWR | O_WRONLY) | O_RDONLY, mode);
        if (fd_buffered_random_ == -1) {
            fprintf(stderr, "FileStore::int_open(): cannot open file: %s: %s\n", pathname, strerror(errno));
            close(fd_buffered_sequential_);
            fd_buffered_sequential_ = -1;
            return false;
        }

        //fprintf(stderr, "use_buffered_io_only_ = %d\n", use_buffered_io_only_);
        if (!use_buffered_io_only_)
            fd_direct_random_ = open(pathname, flags & ~(O_CREAT | ~O_TRUNC | O_RDWR | O_WRONLY) | O_RDONLY | O_DIRECT, mode);
        else {
            fd_direct_random_ = open(pathname, flags & ~(O_CREAT | ~O_TRUNC | O_RDWR | O_WRONLY) | O_RDONLY, mode);
        }
        if (fd_direct_random_ == -1) {
            fprintf(stderr, "FileStore::int_open(): cannot open file: %s: %s\n", pathname, strerror(errno));
            close(fd_buffered_sequential_);
            close(fd_buffered_random_);
            fd_buffered_sequential_ = -1;
            fd_buffered_random_ = -1;
            return false;
        }

        int ret;
        int zero = 0;
        (void)zero;

#ifdef __APPLE__
        ret = fcntl(fd_buffered_random_, F_RDAHEAD, &zero);
#else
        ret = posix_fadvise(fd_buffered_random_, 0, 0, POSIX_FADV_RANDOM);
#endif
        if (ret) {
            fprintf(stderr, "FileStore::int_open(): cannot modify readahead setting for file: %s: %s\n", pathname, strerror(errno));
            close(fd_buffered_sequential_);
            close(fd_buffered_random_);
            close(fd_direct_random_);
            fd_buffered_sequential_ = -1;
            fd_buffered_random_ = -1;
            fd_direct_random_ = -1;
            return false;
        }

#ifdef __APPLE__
        ret = fcntl(fd_direct_random_, F_RDAHEAD, &zero);
#else
        ret = posix_fadvise(fd_direct_random_, 0, 0, POSIX_FADV_RANDOM);
#endif
        if (ret) {
            fprintf(stderr, "FileStore::int_open(): cannot modify readahead setting for file: %s: %s\n", pathname, strerror(errno));
            close(fd_buffered_sequential_);
            close(fd_buffered_random_);
            close(fd_direct_random_);
            fd_buffered_sequential_ = -1;
            fd_buffered_random_ = -1;
            fd_direct_random_ = -1;
            return false;
        }

        return true;
    }

    bool
    FileStore::int_is_open() const
    {
        return fd_buffered_sequential_ != -1;
    }

    bool
    FileStore::int_close()
    {
        // lock must be acquire before checking if the file is open in order to avoid a race condition with int_sync()
        tbb::queuing_mutex::scoped_lock sync_lock(sync_mutex_);

        if (!int_is_open())
            return false;

        if (close(fd_buffered_sequential_) == -1)
            fprintf(stderr, "FileStore::int_unlink(): cannot close file: %s\n", strerror(errno));
        if (close(fd_buffered_random_) == -1)
            fprintf(stderr, "FileStore::int_unlink(): cannot close file: %s\n", strerror(errno));
        if (close(fd_direct_random_) == -1)
            fprintf(stderr, "FileStore::int_unlink(): cannot close file: %s\n", strerror(errno));
        fd_buffered_sequential_ = -1;
        fd_buffered_random_ = -1;
        fd_direct_random_ = -1;

        dirty_chunk_.clear();
        syncing_chunk_.clear();

        memset(cache_, 0, sizeof(cache_entry) * cache_size_);

        return true;
    }

    bool
    FileStore::int_unlink(const char* pathname)
    {
        if (unlink(pathname)) {
            fprintf(stderr, "FileStore::int_unlink(): cannot unlink file: %s: %s\n", pathname, strerror(errno));
            return false;
        }
        return true;
    }

    bool
    FileStore::int_pread(char* buf, size_t& count, off_t offset, bool readahead) const
    {
        if (!int_is_open())
            return false;

        // extend the beginning of the read region to the latest eariler page boundary
        size_t to_discard = offset & page_size_mask_;

        off_t req_offset = offset - to_discard;
        size_t req_count = to_discard + count;

        // extend the end of the read region to the eariest later page boundary
        req_count = (req_count + page_size_mask_) & ~page_size_mask_;

        // sanity check
        assert((req_offset & page_size_mask_) == 0);
        assert((req_count & page_size_mask_) == 0);
        assert(req_offset <= offset);
        assert(req_count >= count);

        // allocate aligned memory
        void* req_buf;
        if (posix_memalign(&req_buf, page_size_, req_count)) {
            fprintf(stderr, "FileStore::int_pread(): cannot allocate aligned memory: %s\n", strerror(errno));
            return false;
        }
        //fprintf(stderr, "FileStore::int_pread(): req_buf == %p (%zu B)\n", req_buf, req_count);

        // use cache if available
        bool use_cache = false;
        if (req_count <= page_size_ * cache_size_) {
            bool cache_available = true;
            tbb::queuing_rw_mutex::scoped_lock cache_lock(cache_mutex_, false);
            for (off_t current_offset = req_offset; current_offset < req_offset + req_count; current_offset += page_size_) {
                size_t cache_index = (current_offset / page_size_) % cache_size_;
                if (!cache_[cache_index].valid ||
                    cache_[cache_index].offset != current_offset) {
                    cache_available = false;
                    break;
                }
            }
            if (cache_available) {
                use_cache = true;
                char* p = static_cast<char*>(req_buf);
                for (off_t current_offset = req_offset; current_offset < req_offset + req_count; current_offset += page_size_) {
                    size_t cache_index = (current_offset / page_size_) % cache_size_;
                    memcpy(p, cache_[cache_index].data, page_size_);
                    p += page_size_;
                }
                //++cache_hit_;
            }
            else {
                //++cache_miss_;
            }
        }

        ssize_t read_len;
        if (use_cache) {
            read_len = req_count;
        }
        else {
            // choose an appropriate file descriptor
            int fd;
            {
                tbb::queuing_rw_mutex::scoped_lock dirty_chunk_lock(dirty_chunk_mutex_, false);

                if (readahead)
                    fd = fd_buffered_sequential_;
                else {
                    if (req_offset / chunk_size_ < dirty_chunk_.size() &&
                        dirty_chunk_[req_offset / chunk_size_]) {
                        // the chunk is marked as dirty -- direct I/O cannot be used
                        fd = fd_buffered_random_;
                    }
                    else
                        fd = fd_direct_random_;
                }
            }

            // do read
            //fprintf(stderr, "for %zu,%zu, requested %zu,%zu\n", offset, count, req_offset, req_count);
            read_len = pread(fd, req_buf, req_count, req_offset);

            if (read_len < 0) {
                fprintf(stderr, "FileStore::int_pread(): cannot read: %s\n", strerror(errno));
                return false;
            }

            // put to cache
            if (req_count <= page_size_ * cache_size_) {
                tbb::queuing_rw_mutex::scoped_lock cache_lock(cache_mutex_, true);
                char* p = static_cast<char*>(req_buf);
                for (off_t current_offset = req_offset; current_offset < req_offset + (read_len - (read_len & page_size_mask_)); current_offset += page_size_) {
                    size_t cache_index = (current_offset / page_size_) % cache_size_;
                    cache_[cache_index].valid = true;
                    cache_[cache_index].offset = current_offset;
                    memcpy(cache_[cache_index].data, p, page_size_);
                    p += page_size_;
                }
            }
        }

        if (read_len < to_discard) {
            // no useful data read
            //fprintf(stderr, "none read\n");
            count = 0;
        }
        else if (read_len < to_discard + count) {
            // partially read
            //fprintf(stderr, "partially read, discarding first %zu and last %zu\n", to_discard, count - to_discard - (read_len - to_discard));
            memcpy(buf, static_cast<char*>(req_buf) + to_discard, read_len - to_discard);
            count = read_len - to_discard;
        }
        else {
            // fully read
            //fprintf(stderr, "fully read, discarding first %zu and last %zu\n", to_discard, count - to_discard - (len));
            memcpy(buf, static_cast<char*>(req_buf) + to_discard, count);
        }

        free(req_buf);

        return true;
    }

    bool
    FileStore::int_pwritev(const struct iovec* iov, int count, off_t offset)
    {
        if (!int_is_open())
            return false;

        size_t write_size = 0;
        for (int i = 0; i < count; i++)
            write_size += iov[i].iov_len;

        size_t start_chunk = offset / chunk_size_;
        size_t end_chunk = (offset + write_size + chunk_size_ - 1) / chunk_size_ + 1;

        // mark affected chunks dirty and unmark syncing chunks
        {
            tbb::queuing_rw_mutex::scoped_lock dirty_chunk_lock(dirty_chunk_mutex_, false);

            for (size_t chunk = start_chunk; chunk < end_chunk; chunk++) {
                if (chunk < dirty_chunk_.size())
                    dirty_chunk_[chunk] = true;
                if (chunk < syncing_chunk_.size())
                    syncing_chunk_[chunk] = false;
            }
        }

        // do write
        ssize_t written_len = pwritev(fd_buffered_sequential_, iov, count, offset);

        // mark affected chunks dirty
        {
            tbb::queuing_rw_mutex::scoped_lock dirty_chunk_lock(dirty_chunk_mutex_, true);

            if (end_chunk <= dirty_chunk_.size()) {
                for (size_t chunk = start_chunk; chunk < end_chunk; chunk++)
                    dirty_chunk_[chunk] = true;
            }
            else {
                for (size_t chunk = start_chunk; chunk < dirty_chunk_.size(); chunk++)
                    dirty_chunk_[chunk] = true;
                dirty_chunk_.resize(end_chunk, true);
            }
        }

        // invalidate cache
        {
            tbb::queuing_rw_mutex::scoped_lock cache_lock(cache_mutex_, true);
            off_t aligned_offset = offset - (offset & page_size_mask_);
            for (off_t current_offset = aligned_offset; current_offset < offset + written_len; current_offset += page_size_) {
                size_t cache_index = (current_offset / page_size_) % cache_size_;
                if (cache_[cache_index].valid && cache_[cache_index].offset == current_offset)
                    cache_[cache_index].valid = false;
            }
        }
        
        if (written_len < write_size) {
            fprintf(stderr, "FileStore::int_pwritev(): cannot write: %s\n", strerror(errno));
            return false;
        }

        return true;
    }

    bool
    FileStore::int_sync(bool blocking)
    {
        // write all buffered writes to flash/disk
        // note: this does not block (i.e. asynchronous version of [synchronization of write buffer])

        // lock must be acquire before checking if the file is open in order to avoid a race condition with int_close()
        tbb::queuing_mutex::scoped_lock* sync_lock = new tbb::queuing_mutex::scoped_lock();

        if (blocking)
            sync_lock->acquire(sync_mutex_);
        else {
            if (!sync_lock->try_acquire(sync_mutex_)) {
                // another syncing is ongoing
                delete sync_lock;
                return true;
            }
        }

        if (!int_is_open()) {
            delete sync_lock;
            return false;
        }

        if (dirty_chunk_.none()) {
            delete sync_lock;
            return true;
        }

        SyncTask* t = new SyncTask();
        t->file_store = this;
        t->sync_lock = sync_lock;
        task_scheduler_sync_.enqueue_task(t);

        if (blocking) {
            // wait for syncing to complete
            tbb::queuing_mutex::scoped_lock blocking_sync_lock(sync_mutex_);
        }

        return true;
    }

    FileStore::SyncTask::~SyncTask()
    {
        delete sync_lock;
        sync_lock = NULL;
    }

    void
    FileStore::SyncTask::Run()
    {
        {
            tbb::queuing_rw_mutex::scoped_lock dirty_chunk_lock(file_store->dirty_chunk_mutex_, false);
            file_store->syncing_chunk_ = file_store->dirty_chunk_;
        }

        if (fdatasync(file_store->fd_buffered_sequential_)) {
            fprintf(stderr, "FileStore::int_pread(): cannot sync: %s\n", strerror(errno));
            return;
        }

        {
            tbb::queuing_rw_mutex::scoped_lock dirty_chunk_lock(file_store->dirty_chunk_mutex_, true);
            file_store->syncing_chunk_.resize(file_store->dirty_chunk_.size(), false);
            file_store->dirty_chunk_ -= file_store->syncing_chunk_;
        }
    }

    TaskScheduler FileStore::task_scheduler_sync_(1, 100);

} // namespace fawn
