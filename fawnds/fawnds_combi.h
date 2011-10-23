/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNDS_COMBI_H_
#define _FAWNDS_COMBI_H_

#include "fawnds.h"
#include "task.h"
#include <tbb/queuing_rw_mutex.h>

namespace fawn {

    // configuration
    //   <type>: "combi" (fixed)
    //   <id>: the ID of the store
    //   <file>: the file name prefix to store bookkeeping information for persistency
    //   <key-len>: the length of keys -- zero for variable-length keys (default), a positive integer for fixed-length keys
    //   <data-len>: the length of data -- zero for variable-length data (default), a positive integer for fixed-length data
    //   <temp-file>: the path of the temporary files/directory. "/tmp" is the default.
    //   <stage-limit>: limit the use of stages up to the specific number (e.g. 1: no store2 will be used); will still need to specify the configuration for all stores.  2 for no limit (default)
    //   <store0-high-watermark>: the number of front stores to begin conversion to a middle store.  2 is default.
    //   <store0-low-watermark>: the number of front stores to stop conversion to a middle store.  1 is default.  Must be at least 1 due to the writable store. store. store. store.
    //   <store1-high-watermark>: the number of middle stores to begin merge into the back store.  1 is default.
    //   <store1-low-watermark>: the number of middle stores to stop merge into the back store.  0 is default.
    //   <store0>: the configuration for front stores
    //             <id>: will be assigned by FawnDS_Combi
    //             <key-len>: will be set by FawnDS_Combi
    //             <data-len>: will be set by FawnDS_Combi
    //   <store1>: the configuration for middle stores
    //             <id>: will be assigned by FawnDS_Combi
    //             <key-len>: will be set by FawnDS_Combi
    //             <data-len>: will be set by FawnDS_Combi
    //   <store2>: the configuration for back stores
    //             <id>: will be assigned by FawnDS_Combi
    //             <key-len>: will be set by FawnDS_Combi
    //             <data-len>: will be set by FawnDS_Combi
    //             <size>: will be set by FawnDS_Combi

    class FawnDS_Combi : public FawnDS {
    public:
        FawnDS_Combi();
        virtual ~FawnDS_Combi();

        virtual FawnDS_Return Create();
        virtual FawnDS_Return Open();

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

        //virtual FawnDS_ConstIterator Find(const ConstValue& key) const;
        //virtual FawnDS_Iterator Find(const ConstValue& key);
    
        struct IteratorElem : public FawnDS_IteratorElem {
            IteratorElem(const FawnDS_Combi* fawnds);
            ~IteratorElem();

            FawnDS_IteratorElem* Clone() const;
            void Next();

            tbb::queuing_rw_mutex::scoped_lock* lock;
            size_t current_stage;
            size_t current_store;
            FawnDS_Iterator store_it;
        };

    protected:
        FawnDS* alloc_store(size_t stage, size_t size = -1);

        class ConvertTask : public Task {
        public:
            virtual void Run();
            FawnDS_Combi* fawnds;

            FawnDS* convert(FawnDS* front_store);
        };

        class MergeTask : public Task {
        public:
            virtual void Run();
            FawnDS_Combi* fawnds;

            FawnDS* sort(FawnDS* sorter, FawnDS* middle_store, size_t& num_adds, size_t& num_dels);
            FawnDS* merge(FawnDS* back_store, FawnDS* sorter, size_t& num_adds, size_t& num_dels);
        };

    private:
        bool open_;

        std::string id_;

        size_t key_len_;
        size_t data_len_;

        std::string temp_file_;

        size_t stage_limit_;

        size_t store0_high_watermark_;
        size_t store0_low_watermark_;

        size_t store1_high_watermark_;
        size_t store1_low_watermark_;

        mutable tbb::queuing_rw_mutex mutex_;

        std::vector<std::vector<FawnDS*> > all_stores_;     // protected by mutex_
        std::vector<size_t> next_ids_;                      // protected by mutex_

        size_t back_store_size_;

        tbb::atomic<bool> convert_task_running_;    // also protected by mutex_
        tbb::atomic<bool> merge_task_running_;      // also protected by mutex_

        friend class MergeTask;

        static TaskScheduler task_scheduler_convert_;
        static TaskScheduler task_scheduler_merge_;

        static const size_t latency_track_store_count_ = 100;
        mutable tbb::atomic<uint64_t> latencies_[4][latency_track_store_count_];
        mutable tbb::atomic<uint64_t> counts_[4][latency_track_store_count_];
    };

} // namespace fawn

#endif  // #ifndef _FAWNDS_COMBI_H_
