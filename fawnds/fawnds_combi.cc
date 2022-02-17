/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "fawnds_combi.h"
#include "fawnds_factory.h"
#include "debug.h"
#include "global_limits.h"
#include <cassert>
//#include "print.h"

#include <sys/time.h>
#include <stdio.h>
#include <sstream>
#include <time.h>
#include <atomic>

namespace fawn {

    FawnDS_Combi::FawnDS_Combi()
        : open_(false)
    {
        for (size_t stage = 0; stage < 4; stage++) {
            for (size_t i = 0; i < latency_track_store_count_; i++) {
                latencies_[stage][i] = 0;
                counts_[stage][i] = 0;
            }
        }
    }

    FawnDS_Combi::~FawnDS_Combi()
    {
        if (open_)
            Close();
    }

    FawnDS_Return
    FawnDS_Combi::Create()
    {
        if (open_)
            return ERROR;

        id_ = config_->GetStringValue("child::id");

        if (config_->ExistsNode("child::key-len") == 0)
            key_len_ = atoi(config_->GetStringValue("child::key-len").c_str());
        else
            key_len_ = 0;

        if (config_->ExistsNode("child::data-len") == 0)
            data_len_ = atoi(config_->GetStringValue("child::data-len").c_str());
        else
            data_len_ = 0;

        if (config_->ExistsNode("child::temp-file") == 0)
            temp_file_ = config_->GetStringValue("child::temp-file");
        else
            temp_file_ = "/tmp";

        if (config_->ExistsNode("child::stage-limit") == 0)
            stage_limit_ = atoi(config_->GetStringValue("child::stage-limit").c_str());
        else
            stage_limit_ = 2;

        if (config_->ExistsNode("child::store0-high-watermark") == 0)
            store0_high_watermark_ = atoi(config_->GetStringValue("child::store0-high-watermark").c_str());
        else
            store0_high_watermark_ = 2;

        if (config_->ExistsNode("child::store0-low-watermark") == 0)
            store0_low_watermark_ = atoi(config_->GetStringValue("child::store0-low-watermark").c_str());
        else
            store0_low_watermark_ = 1;
        if (store0_low_watermark_ < 1 || store0_high_watermark_ <= store0_low_watermark_)
            return ERROR;

        if (config_->ExistsNode("child::store1-high-watermark") == 0)
            store1_high_watermark_ = atoi(config_->GetStringValue("child::store1-high-watermark").c_str());
        else
            store1_high_watermark_ = 1;

        if (config_->ExistsNode("child::store1-low-watermark") == 0)
            store1_low_watermark_ = atoi(config_->GetStringValue("child::store1-low-watermark").c_str());
        else
            store1_low_watermark_ = 0;
        if (store1_high_watermark_ <= store1_low_watermark_)
            return ERROR;

        next_ids_.push_back(0);
        next_ids_.push_back(0);
        next_ids_.push_back(0);

        all_stores_.push_back(std::vector<FawnDS*>());
        all_stores_.push_back(std::vector<FawnDS*>());
        all_stores_.push_back(std::vector<FawnDS*>());

        all_stores_[0].push_back(alloc_store(0));
        all_stores_[0].back()->Create();

        back_store_size_ = 0;

        convert_task_running_ = false;
        merge_task_running_ = false;

        open_ = true;

        return OK;
    }

    FawnDS_Return
    FawnDS_Combi::Open()
    {
        if (open_)
            return ERROR;

        // TODO: store and load next IDs for persistency

        id_ = config_->GetStringValue("child::id");

        if (config_->ExistsNode("child::key-len") == 0)
            key_len_ = atoi(config_->GetStringValue("child::key-len").c_str());
        else
            key_len_ = 0;

        if (config_->ExistsNode("child::data-len") == 0)
            data_len_ = atoi(config_->GetStringValue("child::data-len").c_str());
        else
            data_len_ = 0;

        if (config_->ExistsNode("child::temp-file") == 0)
            temp_file_ = config_->GetStringValue("child::temp-file");
        else
            temp_file_ = "/tmp";

        if (config_->ExistsNode("child::stage-limit") == 0)
            stage_limit_ = atoi(config_->GetStringValue("child::stage-limit").c_str());
        else
            stage_limit_ = 2;

        if (config_->ExistsNode("child::store0-high-watermark") == 0)
            store0_high_watermark_ = atoi(config_->GetStringValue("child::store0-high-watermark").c_str());
        else
            store0_high_watermark_ = 2;

        if (config_->ExistsNode("child::store0-low-watermark") == 0)
            store0_low_watermark_ = atoi(config_->GetStringValue("child::store0-low-watermark").c_str());
        else
            store0_low_watermark_ = 1;
        if (store0_low_watermark_ < 1 || store0_high_watermark_ <= store0_low_watermark_)
            return ERROR;

        if (config_->ExistsNode("child::store1-high-watermark") == 0)
            store1_high_watermark_ = atoi(config_->GetStringValue("child::store1-high-watermark").c_str());
        else
            store1_high_watermark_ = 1;

        if (config_->ExistsNode("child::store1-low-watermark") == 0)
            store1_low_watermark_ = atoi(config_->GetStringValue("child::store1-low-watermark").c_str());
        else
            store1_low_watermark_ = 0;
        if (store1_high_watermark_ <= store1_low_watermark_)
            return ERROR;

        next_ids_.push_back(0);
        next_ids_.push_back(0);
        next_ids_.push_back(0);

        all_stores_.push_back(std::vector<FawnDS*>());
        all_stores_.push_back(std::vector<FawnDS*>());
        all_stores_.push_back(std::vector<FawnDS*>());

        all_stores_[0].push_back(alloc_store(0));
        all_stores_[0].back()->Create();

        back_store_size_ = 0;

        convert_task_running_ = false;
        merge_task_running_ = false;

        open_ = true;

        return OK;
    }

    FawnDS_Return
    FawnDS_Combi::Flush()
    {
        if (!open_)
            return ERROR;

        {
            tbb::queuing_rw_mutex::scoped_lock lock(mutex_, false);

            FawnDS_Return ret = all_stores_[0][0]->Flush();
            if (ret != OK)
                return ret;
        }

        GlobalLimits::instance().disable();

        {
            tbb::queuing_rw_mutex::scoped_lock lock;
            RateLimiter rate_limiter(0, 1, 1, 10000000L); // poll status every 10 ms or more

            // ensure all tasks to complete
            // note: Flush() may fall in a livelock state if there are many pending operations
            while (true) {
                if (lock.try_acquire(mutex_, true)) {
                    if (!convert_task_running_ && !merge_task_running_) {
                        if (!(
                                (stage_limit_ >= 1 && all_stores_[0].size() >= store0_high_watermark_) ||
                                (stage_limit_ >= 2 && all_stores_[1].size() >= store1_high_watermark_)
                            ))
                            break;
                    }
                    lock.release();
                }

                rate_limiter.remove_tokens(1);
            }
        }

        GlobalLimits::instance().enable();

        struct timeval tv;
        if (gettimeofday(&tv, NULL))
            assert(false);

        for (size_t stage = 0; stage < 4; stage++) {
            for (size_t i = 0; i < latency_track_store_count_; i++) {
                if (counts_[stage][i] != 0) {
                    fprintf(stdout, "%llu.%06llu: (%s) latency: stage: %zu, i: %zu, sum: %llu ns, count: %llu\n",
                            static_cast<long long unsigned>(tv.tv_sec),
                            static_cast<long long unsigned>(tv.tv_usec),
                            id_.c_str(),
                            stage, i,
                            static_cast<long long unsigned>(latencies_[stage][i]),
                            static_cast<long long unsigned>(counts_[stage][i]));
                }
                latencies_[stage][i] = 0;
                counts_[stage][i] = 0;
            }
        }
        fflush(stdout);

        return OK;
    }

    FawnDS_Return
    FawnDS_Combi::Close()
    {
        if (!open_)
            return ERROR;

        FawnDS_Return ret = Flush();
        if (ret != OK)
            return ret;

        for (size_t stage = 0; stage < all_stores_.size(); stage++) {
            for (size_t i = 0; i < all_stores_[stage].size(); i++) {
                all_stores_[stage][i]->Close();
                delete all_stores_[stage][i];
            }
        }

        all_stores_.clear();

        open_ = false;

        return OK;
    }

    FawnDS_Return
    FawnDS_Combi::Destroy()
    {
        // TODO: implement

        return UNSUPPORTED;
    }

	FawnDS_Return
	FawnDS_Combi::Status(const FawnDS_StatusType& type, Value& status) const
	{
        if (!open_)
            return ERROR;

        tbb::queuing_rw_mutex::scoped_lock lock(mutex_, false);

        std::ostringstream oss;
        switch (type) {
        case NUM_DATA:
        case NUM_ACTIVE_DATA:
        case MEMORY_USE:
        case DISK_USE:
            {
                Value status_part;
                oss << '[';
                for (size_t stage = 0; stage < all_stores_.size(); stage++) {
                    if (stage != 0)
                        oss << ',';
                    oss << '[';
                    for (size_t i = 0; i < all_stores_[stage].size(); i++) {
                        if (i != 0)
                            oss << ',';
                        FawnDS_Return ret = all_stores_[stage][i]->Status(type, status_part);
                        if (ret != OK)
                            return UNSUPPORTED;
                        oss << status_part.str();
                    }
                    oss << ']';
                }
                oss << ']';
            }
            break;
		case CAPACITY:
            oss << -1;      // unlimited
            break;
        default:
            return UNSUPPORTED;
        }
        status = NewValue(oss.str());
        return OK;
    }

    FawnDS_Return
    FawnDS_Combi::Put(const ConstValue& key, const ConstValue& data)
    {
        if (!open_)
            return ERROR;

        if (key_len_ != key.size())
            return INVALID_KEY;

        if (data_len_ != data.size())
            return INVALID_DATA;

        FawnDS_Return ret;
        while (true) {
            tbb::queuing_rw_mutex::scoped_lock lock(mutex_, false);
            FawnDS* front_store = all_stores_[0][0];

            ret = front_store->Put(key, data);
            if (ret != INSUFFICIENT_SPACE)
                break;

            lock.upgrade_to_writer();

            if (front_store != all_stores_[0][0]) {
                // other thread already made a new front store
                continue;
            }

            FawnDS* new_store = alloc_store(0);
            new_store->Create();

            all_stores_[0].insert(all_stores_[0].begin(), new_store);

            if (stage_limit_ >= 1 &&
                !convert_task_running_ &&
                all_stores_[0].size() >= store0_high_watermark_) {
                convert_task_running_ = true;

                ConvertTask* t = new ConvertTask();
                t->fawnds = this;
                task_scheduler_convert_.enqueue_task(t);
            }
        }

        return ret;
    }

    FawnDS_Return
    FawnDS_Combi::Append(Value& key, const ConstValue& data)
    {
        if (!open_)
            return ERROR;

        if (data_len_ != data.size())
            return INVALID_DATA;

        FawnDS_Return ret;
        while (true) {
            tbb::queuing_rw_mutex::scoped_lock lock(mutex_, false);
            FawnDS* front_store = all_stores_[0][0];

            ret = front_store->Append(key, data);
            if (ret != INSUFFICIENT_SPACE)
                break;

            lock.upgrade_to_writer();

            if (front_store != all_stores_[0][0]) {
                // other thread already made a new front store
                continue;
            }

            FawnDS* new_store = alloc_store(0);
            new_store->Create();

            all_stores_[0].insert(all_stores_[0].begin(), new_store);

            if (stage_limit_ >= 1 &&
                !convert_task_running_ &&
                all_stores_[0].size() >= store0_high_watermark_) {
                convert_task_running_ = true;

                ConvertTask* t = new ConvertTask();
                t->fawnds = this;
                task_scheduler_convert_.enqueue_task(t);
            }
        }

        return ret;
    }

    FawnDS_Return
    FawnDS_Combi::Delete(const ConstValue& key)
    {
        if (key_len_ != key.size())
            return INVALID_KEY;

        tbb::queuing_rw_mutex::scoped_lock lock(mutex_, false); // Delete() does not change the all_stores_ structure

        FawnDS_Return ret = all_stores_[0][0]->Delete(key);
        if (ret == OK || ret == KEY_NOT_FOUND)
            return OK;
        else
            return ret;
    }

    FawnDS_Return
    FawnDS_Combi::Contains(const ConstValue& key) const
    {
        if (key_len_ != key.size())
            return INVALID_KEY;

        tbb::queuing_rw_mutex::scoped_lock lock(mutex_, false);

        for (size_t stage = 0; stage < all_stores_.size(); stage++) {
            for (size_t i = 0; i < all_stores_[stage].size(); i++) {
                FawnDS_Return ret = all_stores_[stage][i]->Contains(key);
                if (ret != KEY_NOT_FOUND)
                    return ret;
            }
        }

        return KEY_NOT_FOUND;
    }

    FawnDS_Return
    FawnDS_Combi::Length(const ConstValue& key, size_t& len) const
    {
        if (key_len_ != key.size())
            return INVALID_KEY;

        tbb::queuing_rw_mutex::scoped_lock lock(mutex_, false);

        for (size_t stage = 0; stage < all_stores_.size(); stage++) {
            for (size_t i = 0; i < all_stores_[stage].size(); i++) {
                FawnDS_Return ret = all_stores_[stage][i]->Length(key, len);
                if (ret != KEY_NOT_FOUND)
                    return ret;
            }
        }

        return KEY_NOT_FOUND;
    }

    FawnDS_Return
    FawnDS_Combi::Get(const ConstValue& key, Value& data, size_t offset, size_t len) const
    {
        struct timespec ts;

        clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
        int64_t last_time = static_cast<int64_t>(ts.tv_sec) * 1000000000Lu + static_cast<int64_t>(ts.tv_nsec);

        if (key_len_ != key.size())
            return INVALID_KEY;

        tbb::queuing_rw_mutex::scoped_lock lock(mutex_, false);

        for (size_t stage = 0; stage < all_stores_.size(); stage++) {
            for (size_t i = 0; i < all_stores_[stage].size(); i++) {
                FawnDS_Return ret = all_stores_[stage][i]->Get(key, data, offset, len);
                if (ret != KEY_NOT_FOUND) {
                    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
                    int64_t current_time = static_cast<int64_t>(ts.tv_sec) * 1000000000Lu + static_cast<int64_t>(ts.tv_nsec);
                    if (stage < 3 && i < latency_track_store_count_) {
                        latencies_[stage][i] += current_time - last_time;
                        ++counts_[stage][i];
                    }

                    return ret;
                }
            }
        }

        clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
        int64_t current_time = static_cast<int64_t>(ts.tv_sec) * 1000000000Lu + static_cast<int64_t>(ts.tv_nsec);
        latencies_[3][0] += current_time - last_time;
        ++counts_[3][0];

        return KEY_NOT_FOUND;
    }

    FawnDS_ConstIterator
    FawnDS_Combi::Enumerate() const
    {
        IteratorElem* elem = new IteratorElem(this);
        elem->current_stage = 0;
        elem->current_store = static_cast<size_t>(-1);
        elem->Next();
        return FawnDS_ConstIterator(elem);
    }

    FawnDS_Iterator
    FawnDS_Combi::Enumerate()
    {
        IteratorElem* elem = new IteratorElem(this);
        elem->current_stage = 0;
        elem->current_store = static_cast<size_t>(-1);
        elem->Next();
        return FawnDS_Iterator(elem);
    }

    FawnDS*
    FawnDS_Combi::alloc_store(size_t stage, size_t size)
    {
        char buf[1024];
        snprintf(buf, sizeof(buf), "%zu", stage);

        Configuration* config = new Configuration(config_, true);
        config->SetContextNode(std::string("child::store") + buf);

        snprintf(buf, sizeof(buf), "%s_%zu", config_->GetStringValue("child::id").c_str(), next_ids_[stage]++);
        config->SetStringValue("child::id", buf);

        snprintf(buf, sizeof(buf), "%zu", key_len_);
        config->SetStringValue("child::key-len", buf);

        snprintf(buf, sizeof(buf), "%zu", data_len_);
        config->SetStringValue("child::data-len", buf);

        if (size != static_cast<size_t>(-1)) {
            snprintf(buf, sizeof(buf), "%zu", size);
            config->SetStringValue("child::size", buf);
        }

        FawnDS* store = FawnDS_Factory::New(config);
        if (store == NULL) {
            DPRINTF(2, "failed to allocate new store\n");
            delete config;
        }
        return store;
    }

    void
    FawnDS_Combi::ConvertTask::Run()
    {
        //fprintf(stderr, "FawnDS_Combi::ConvertTask::Run(): converting\n");

        FawnDS* front_store;
        FawnDS* middle_store;

        //fprintf(stderr, "FawnDS_Combi::ConvertTask::Run(): 1\n");

        // get the front store
        {
            tbb::queuing_rw_mutex::scoped_lock lock(fawnds->mutex_, false);

            assert(fawnds->all_stores_[0].size() > fawnds->store0_low_watermark_);

            front_store = fawnds->all_stores_[0].back();
        }

        //fprintf(stderr, "FawnDS_Combi::ConvertTask::Run(): 2\n");

        // convert to the middle store
        {
            Value status;
            size_t num_data = 0;
            if (front_store->Status(NUM_DATA, status) == OK)
                num_data = atoll(status.str().c_str());
            //size_t expected_IO = (fawnds->key_len_ + fawnds->data_len_) * num_data;

            middle_store = convert(front_store);
        }

        //fprintf(stderr, "FawnDS_Combi::ConvertTask::Run(): 3\n");

        // remove the front store and insert the middle store
        {
            tbb::queuing_rw_mutex::scoped_lock lock(fawnds->mutex_, true);

            //fprintf(stderr, "FawnDS_Combi::ConvertTask::Run(): 4\n");
            fawnds->all_stores_[0].pop_back();
            fawnds->all_stores_[1].insert(fawnds->all_stores_[1].begin(), middle_store);

            // check if merge is necessary
            if (fawnds->stage_limit_ >= 2 &&
                !fawnds->merge_task_running_ &&
                fawnds->all_stores_[1].size() >= fawnds->store1_high_watermark_) {
                fawnds->merge_task_running_ = true;

                MergeTask* t = new MergeTask();
                t->fawnds = fawnds;
                fawnds->task_scheduler_merge_.enqueue_task(t);
            }
        }

        // destroy the front store
        {
            front_store->Close();
            front_store->Destroy();
            delete front_store;
        }

        {
            tbb::queuing_rw_mutex::scoped_lock lock(fawnds->mutex_, true);

            if (fawnds->all_stores_[0].size() > fawnds->store0_low_watermark_) {
                // requeue
                ConvertTask* t = new ConvertTask();
                t->fawnds = fawnds;
                fawnds->task_scheduler_convert_.enqueue_task(t);
            }
            else {
                // no more store to convert
                assert(fawnds->convert_task_running_);
                fawnds->convert_task_running_ = false;
            }
        }

        //fprintf(stderr, "FawnDS_Combi::ConvertTask::Run(): converting done\n");
    }

    FawnDS*
    FawnDS_Combi::ConvertTask::convert(FawnDS* front_store)
    {
        {
            struct timeval tv;
            if (gettimeofday(&tv, NULL)) {
                perror("Error while getting the current time");
            }

            Value status;
            front_store->Status(NUM_ACTIVE_DATA, status);

            fprintf(stdout, "%llu.%06llu: (%s) conversion started: %s from front store\n",
                    static_cast<long long unsigned>(tv.tv_sec),
                    static_cast<long long unsigned>(tv.tv_usec),
                    fawnds->id_.c_str(),
                    status.str().c_str()
                );
            fflush(stdout);
        }

        // locking for alloc_store is unnecessary because there is only one thread that modifies the specific value
        FawnDS* middle_store = fawnds->alloc_store(1);
        if (middle_store->Create() != OK) {
            fprintf(stderr, "Error while creating a middle store\n");
            assert(false);
            delete middle_store;
            return NULL;
        }

        FawnDS_Return ret = front_store->ConvertTo(middle_store);
        assert(ret == OK);

        {
            struct timeval tv;
            if (gettimeofday(&tv, NULL)) {
                perror("Error while getting the current time");
            }

            Value status;
            middle_store->Status(NUM_ACTIVE_DATA, status);

            fprintf(stdout, "%llu.%06llu: (%s) conversion finished: %s entries to middle store\n",
                    static_cast<long long unsigned>(tv.tv_sec),
                    static_cast<long long unsigned>(tv.tv_usec),
                    fawnds->id_.c_str(),
                    status.str().c_str()
                );
            fflush(stdout);
        }

        return middle_store;
    }

    void
    FawnDS_Combi::MergeTask::Run()
    {
        //fprintf(stderr, "FawnDS_Combi::MergeTask::Run(): merging\n");

        // check if there is an enough number of middle stores to merge
        {
            tbb::queuing_rw_mutex::scoped_lock lock(fawnds->mutex_, false);

            assert(fawnds->all_stores_[1].size() > fawnds->store1_low_watermark_);
        }

        {
            struct timeval tv;
            if (gettimeofday(&tv, NULL)) {
                perror("Error while getting the current time");
            }
            fprintf(stdout, "%llu.%06llu: (%s) merge started\n",
                    static_cast<long long unsigned>(tv.tv_sec),
                    static_cast<long long unsigned>(tv.tv_usec),
                    fawnds->id_.c_str()
                );
            fflush(stdout);
        }

        size_t num_adds = 0;
        size_t num_dels = 0;

        // sort all available middle stores
        FawnDS* sorter = NULL;
        size_t sorted_middle_stores = 0;
        while (true) {
            FawnDS* middle_store_to_sort;
            {
                tbb::queuing_rw_mutex::scoped_lock lock(fawnds->mutex_, false);
                if (sorted_middle_stores + fawnds->store1_low_watermark_ >= fawnds->all_stores_[1].size()) {
                    // no more middle store to sort
                    break;
                }
                size_t idx = fawnds->all_stores_[1].size() - 1 - sorted_middle_stores;
                middle_store_to_sort = fawnds->all_stores_[1][idx];
            }
            sorted_middle_stores++;

            sorter = sort(sorter, middle_store_to_sort, num_adds, num_dels);
        }

        // obtain the back store
        FawnDS* back_store;
        {
            // don't need to obtain read lock because the current thread is the only thread that can modify fawnds->all_stores_[2]
            if (fawnds->all_stores_[2].size() == 0)
                back_store = NULL;
            else if (fawnds->all_stores_[2].size() == 1)
                back_store = fawnds->all_stores_[2][0];
            else {
                assert(false);
                return;
            }
        }

        // merge the sorted middle stores into the back store 
        FawnDS* new_back_store;
        {
            new_back_store = merge(back_store, sorter, num_adds, num_dels);
        }

        // remove the middle stores and replace the back store
        std::vector<FawnDS*> removed_middle_stores;
        {
            tbb::queuing_rw_mutex::scoped_lock lock(fawnds->mutex_, true);

            for (size_t i = 0; i < sorted_middle_stores; i++) {
                removed_middle_stores.push_back(fawnds->all_stores_[1].back());
                fawnds->all_stores_[1].pop_back();
            }

            fawnds->all_stores_[2].clear();
            fawnds->all_stores_[2].push_back(new_back_store);
        }

        // destroy middle stores and the back store
        {
            for (size_t i = 0; i < sorted_middle_stores; i++) {
                removed_middle_stores[i]->Close();
                removed_middle_stores[i]->Destroy();
                delete removed_middle_stores[i];
            }
            removed_middle_stores.clear();

            if (back_store) {
                back_store->Close();
                back_store->Destroy();
                delete back_store;
                back_store = NULL;
            }
        }

        {
            struct timeval tv;
            if (gettimeofday(&tv, NULL)) {
                perror("Error while getting the current time");
            }
            fprintf(stdout, "%llu.%06llu: (%s) merge finished: %zu entries to back store, %zu entries deleted\n",
                    static_cast<long long unsigned>(tv.tv_sec),
                    static_cast<long long unsigned>(tv.tv_usec),
                    fawnds->id_.c_str(),
                    num_adds,
                    num_dels
                );
            fflush(stdout);
        }

        {
            tbb::queuing_rw_mutex::scoped_lock lock(fawnds->mutex_, true);
            if (fawnds->all_stores_[1].size() > fawnds->store1_low_watermark_) {
                // requeue
                MergeTask* t = new MergeTask();
                t->fawnds = fawnds;
                fawnds->task_scheduler_merge_.enqueue_task(t);
            }
            else {
                // no more store to merge
                assert(fawnds->merge_task_running_);
                fawnds->merge_task_running_ = false;
            }
        }

        //fprintf(stderr, "FawnDS_Combi::MergeTask::Run(): merging done\n");
    }

    FawnDS*
    FawnDS_Combi::MergeTask::sort(FawnDS* sorter, FawnDS* middle_store, size_t& num_adds, size_t& num_dels)
    {
        if (sorter == NULL) {
            Configuration* sorter_config = new Configuration();

            char buf[1024];

            if (sorter_config->CreateNodeAndAppend("type", ".") != 0)
                assert(false);
            if (sorter_config->SetStringValue("type", "sorter") != 0)
                assert(false);

            if (sorter_config->CreateNodeAndAppend("key-len", ".") != 0)
                assert(false);
            snprintf(buf, sizeof(buf), "%zu", fawnds->key_len_);
            if (sorter_config->SetStringValue("key-len", buf) != 0)
                assert(false);

            if (sorter_config->CreateNodeAndAppend("data-len", ".") != 0)
                assert(false);
            snprintf(buf, sizeof(buf), "%zu", 1 + fawnds->data_len_);
            if (sorter_config->SetStringValue("data-len", buf) != 0)
                assert(false);

            if (sorter_config->CreateNodeAndAppend("temp-file", ".") != 0)
                assert(false);
            if (sorter_config->SetStringValue("temp-file", fawnds->temp_file_) != 0)
                assert(false);

            sorter = FawnDS_Factory::New(sorter_config);
            if (!sorter) {
                assert(false);
                delete sorter_config;
                return NULL;
            }
            if (sorter->Create() != OK) {
                assert(false);
                delete sorter;
                return NULL;
            }
        }

        FawnDS_ConstIterator it = middle_store->Enumerate();
        while (!it.IsEnd()) {
            Value combined_data;
            combined_data.resize(1 + fawnds->data_len_);
            combined_data.data()[0] = it.IsKeyDeleted() ? 1 : 0;
            memcpy(combined_data.data() + 1, it->data.data(), it->data.size());

            if (sorter->Put(it->key, combined_data) != OK) {
                assert(false);
                delete sorter;
                return NULL;
            }

            if (!it.IsKeyDeleted())
                num_adds++;
            else
                num_dels++;

            GlobalLimits::instance().remove_merge_tokens(1);
            ++it;
        }

        {
            struct timeval tv;
            if (gettimeofday(&tv, NULL)) {
                perror("Error while getting the current time");
            }
            fprintf(stdout, "%llu.%06llu: (%s) sorting: added more unsorted entries (currently %zu entries to add, %zu entries to delete)\n",
                    static_cast<long long unsigned>(tv.tv_sec),
                    static_cast<long long unsigned>(tv.tv_usec),
                    fawnds->id_.c_str(),
                    num_adds,
                    num_dels
                );
            fflush(stdout);
        }

        return sorter;
    }

    FawnDS*
    FawnDS_Combi::MergeTask::merge(FawnDS* back_store, FawnDS* sorter, size_t& num_adds, size_t& num_dels)
    {
        DPRINTF(2, "FawnDS_Combi::MergeTask::Merge(): sorting middle store entries\n");

        //fprintf(stderr, "FawnDS_Combi::MergeTask::Merge(): %zu entries found in middle stores\n", num_adds + num_dels);

        {
            struct timeval tv;
            if (gettimeofday(&tv, NULL)) {
                perror("Error while getting the current time");
            }
            fprintf(stdout, "%llu.%06llu: (%s) sorting: stopped adding unsorted entries\n",
                    static_cast<long long unsigned>(tv.tv_sec),
                    static_cast<long long unsigned>(tv.tv_usec),
                    fawnds->id_.c_str()
                );
            fflush(stdout);
        }

        if (sorter->Flush() != OK) {
            assert(false);
            delete sorter;
            return NULL;
        }

        DPRINTF(2, "FawnDS_Combi::MergeTask::Merge(): merging sorted entries into the back store with duplicate entry supression\n");

        // note that this is just a guess, not an accurate estimate.
        // the actual size can vary if there are many updates or duplicate deletions.
        ssize_t estimated_new_back_store_size = static_cast<ssize_t>(fawnds->back_store_size_) + static_cast<ssize_t>(num_adds) - static_cast<ssize_t>(num_dels);
        size_t max_new_back_store_size;
        if (estimated_new_back_store_size <= 0)
            max_new_back_store_size = 0;
        else
            max_new_back_store_size = estimated_new_back_store_size;

        // locking for alloc_store is unnecessary because there is only one thread that modifies the specific value
        FawnDS* new_back_store = fawnds->alloc_store(2, max_new_back_store_size);
        if (new_back_store->Create() != OK) {
            fprintf(stderr, "Error while creating a back store\n");
            assert(false);
            delete back_store;
            delete sorter;
            return NULL;
        }

        num_adds = 0;
        num_dels = 0;

        bool first = true;

        {
            FawnDS_ConstIterator it_b;
            if (back_store)
               it_b = back_store->Enumerate();
            FawnDS_ConstIterator it_m = sorter->Enumerate();

            if (first) {
                first = false;

                {
                    struct timeval tv;
                    if (gettimeofday(&tv, NULL)) {
                        perror("Error while getting the current time");
                    }
                    fprintf(stdout, "%llu.%06llu: (%s) sorting: started retrieving sorted entries\n",
                            static_cast<long long unsigned>(tv.tv_sec),
                            static_cast<long long unsigned>(tv.tv_usec),
                            fawnds->id_.c_str()
                        );
                    fflush(stdout);
                }
            }

            while (true) {
                bool select_middle_store;

                if (!it_m.IsEnd()) {
                    if (!it_b.IsEnd()) {
                        int cmp = it_m->key.compare(it_b->key);
                        if (cmp < 0)
                            select_middle_store = true;
                        else if (cmp == 0) {
                            select_middle_store = true;
                            // ignore duplicate key from the back store
                            // by taking the key fro the middle store only
                            GlobalLimits::instance().remove_merge_tokens(1);
                            ++it_b;
                            num_dels++;
                        }
                        else
                            select_middle_store = false;
                    }
                    else
                        select_middle_store = true;
                }
                else {
                    if (!it_b.IsEnd())
                        select_middle_store = false;
                    else
                        break;
                }

                Value key;
                Value data;
                bool deleted;

                if (select_middle_store) {
                    while (true) {
                        key = it_m->key;
                        deleted = it_m->data.data()[0] == 1;
                        if (!deleted)
                            data = NewValue(it_m->data.data() + 1, fawnds->data_len_);
                        else
                            data.resize(0);

                        //GlobalLimits::instance().remove_merge_tokens(1);
                        // this data is from sorter, which uses another storage,
                        // so it should not be rate-limited
                        ++it_m;

                        // ignore duplicate key from the middle store
                        // by taking the last key fro the middle store only
                        if (!it_m.IsEnd() && key == it_m->key) {
                            //fprintf(stderr, "dup key: ");
                            //print_payload((const u_char*)key.data(), key.size(), 0);
                            //if (data == it_m->data)
                            //    fprintf(stderr, "dup data!\n");
                            //else
                            //    fprintf(stderr, "non-dup data!\n");
                            num_dels++;
                            continue;
                        }
                        else
                            break;
                    }
                }
                else {
                    // the back store is assumed to have no duplicate entry
                    key = it_b->key;
                    data = it_b->data;
                    deleted = false;
                    GlobalLimits::instance().remove_merge_tokens(1);
                    ++it_b;
                }

                if (!deleted) {
                    FawnDS_Return ret = new_back_store->Put(key, data);
                    assert(ret == OK);
                    num_adds++;
                }
                else
                    num_dels++;
            }
        }

        delete sorter;
        sorter = NULL;

        //fprintf(stderr, "FawnDS_Combi::MergeTask::Merge(): %zu keys kept, %zu keys deleted\n", num_adds, num_dels);

        DPRINTF(2, "FawnDS_Combi::MergeTask::Merge(): flushing\n");

        new_back_store->Flush();

        fawnds->back_store_size_ = num_adds;

        return new_back_store;
    }

    FawnDS_Combi::IteratorElem::IteratorElem(const FawnDS_Combi* fawnds)
    {
        this->fawnds = fawnds;
        lock = new tbb::queuing_rw_mutex::scoped_lock(fawnds->mutex_, false);
    }

    FawnDS_Combi::IteratorElem::~IteratorElem()
    {
        delete lock;
        lock = NULL;
    }

    FawnDS_IteratorElem*
    FawnDS_Combi::IteratorElem::Clone() const
    {
        IteratorElem* elem = new IteratorElem(static_cast<const FawnDS_Combi*>(fawnds));
        tbb::queuing_rw_mutex::scoped_lock* elem_lock = elem->lock;
        *elem = *this;
        elem->lock = elem_lock;
        return elem;
    }

    void
    FawnDS_Combi::IteratorElem::Next()
    {
        const FawnDS_Combi* fawnds_combi = static_cast<const FawnDS_Combi*>(fawnds);

        if (!store_it.IsEnd()) {
            ++store_it;
        }

        while (store_it.IsEnd()) {
            current_store++;

            while (true) {
                if (current_stage >= fawnds_combi->all_stores_.size()) {
                    state = END;
                    return;
                }

                if (current_store >= fawnds_combi->all_stores_[current_stage].size()) {
                    current_stage++;
                    fprintf(stderr, "FawnDS_Combi::IteratorElem::Next(): enumerating stage %zu\n", current_stage);
                    current_store = 0;
                }
                else
                    break;
            }

            store_it = fawnds_combi->all_stores_[current_stage][current_store]->Enumerate();
        }

        state = store_it->state;
        key = store_it->key;
        data = store_it->data;
    }

    TaskScheduler FawnDS_Combi::task_scheduler_convert_(1, 100, TaskScheduler::CPU_LOW);
    TaskScheduler FawnDS_Combi::task_scheduler_merge_(1, 100, TaskScheduler::CPU_LOW);

} // namespace fawn

