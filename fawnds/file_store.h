/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FILE_STORE_H_
#define _FILE_STORE_H_

#include "fawnds.h"
#include "file_io.h"    // for iovec
#include "task.h"
#include <atomic>
#include <tbb/queuing_mutex.h>
#include <tbb/queuing_rw_mutex.h>
#include <boost/dynamic_bitset.hpp>

namespace fawn {

    // configuration
    //   <type>: "file" (fixed)
    //   <id>: the ID of the file store
    //   <file>: the file name prefix to store log entries for persistence
    //   <data-len>: the length of data -- zero for variable-length data (default), a positive integer for fixed-length data (space optimization is applied)
    //   <use-buffered-io-only>: with a non-zero value, use buffered I/O only and do not use direct I/O.  Default is 0 (false).  Useful for quick tests or data-len >= 4096 (direct I/O is less likely to improve read performance).

    class FileStore : public FawnDS {
    public:
        FileStore();
        virtual ~FileStore();

        virtual FawnDS_Return Create();
        virtual FawnDS_Return Open();

        virtual FawnDS_Return ConvertTo(FawnDS* new_store) const;

        virtual FawnDS_Return Flush();
        virtual FawnDS_Return Close();

        virtual FawnDS_Return Destroy();

        virtual FawnDS_Return Status(const FawnDS_StatusType& type, Value& status) const;

        virtual FawnDS_Return Put(const ConstValue& key, const ConstValue& data);
        virtual FawnDS_Return Append(Value& key, const ConstValue& data);

        //virtual FawnDS_Return Delete(const ConstValue& key);

        virtual FawnDS_Return Contains(const ConstValue& key) const;
        virtual FawnDS_Return Length(const ConstValue& key, size_t& len) const;
        virtual FawnDS_Return Get(const ConstValue& key, Value& data, size_t offset = 0, size_t len = -1) const;

        virtual FawnDS_ConstIterator Enumerate() const;
        virtual FawnDS_Iterator Enumerate();

        virtual FawnDS_ConstIterator Find(const ConstValue& key) const;
        virtual FawnDS_Iterator Find(const ConstValue& key);

        struct IteratorElem : public FawnDS_IteratorElem {
            FawnDS_IteratorElem* Clone() const;
            void Next();

            off_t next_id;
        };

    protected:
        typedef uint32_t entry_length_t;

        FawnDS_Return length(const ConstValue& key, size_t& len, bool readahead) const;
        FawnDS_Return get(const ConstValue& key, Value& data, size_t offset, size_t len, bool readahead) const;

        //int disable_readahead();

        // I/O layer (TODO: make this as a separate class)
        void int_initialize();
        void int_terminate();

        bool int_open(const char* pathname, int flags, mode_t mode);
        bool int_is_open() const;
        bool int_close();
        static bool int_unlink(const char* pathname);

        bool int_pread(char* buf, size_t& count, off_t offset, bool readahead) const;
        bool int_pwritev(const struct iovec* iov, int count, off_t offset);
        bool int_sync(bool blocking);

    private:
        size_t data_len_;

        std::atomic<off_t> next_append_id_;
        off_t end_id_;
        std::atomic<off_t> next_sync_;

        bool use_buffered_io_only_;

        // I/O layer (TODO: make this as a separate class)
        static const size_t chunk_size_ = 1048576;
        static const size_t page_size_ = 512;
        static const size_t page_size_mask_ = page_size_ - 1;
        static const size_t cache_size_ = 128;

        int fd_buffered_sequential_;
        int fd_buffered_random_;
        int fd_direct_random_;

        mutable tbb::queuing_mutex sync_mutex_;
        mutable tbb::queuing_rw_mutex dirty_chunk_mutex_;
        boost::dynamic_bitset<> dirty_chunk_;
        boost::dynamic_bitset<> syncing_chunk_;

        struct cache_entry {
            bool valid;
            off_t offset;
            char data[page_size_];
        };
        mutable tbb::queuing_rw_mutex cache_mutex_;
        mutable cache_entry cache_[cache_size_];
        mutable std::atomic<size_t> cache_hit_;
        mutable std::atomic<size_t> cache_miss_;

        class SyncTask : public Task {
        public:
            virtual ~SyncTask();
            virtual void Run();
            FileStore* file_store;
            tbb::queuing_mutex::scoped_lock* sync_lock;
        };
        static TaskScheduler task_scheduler_sync_;

        friend class SyncTask;
    };

} // namespace fawn

#endif // #ifndef _FILE_STORE_H_
