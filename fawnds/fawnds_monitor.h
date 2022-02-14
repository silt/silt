/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNDS_MONITOR_H_
#define _FAWNDS_MONITOR_H_

#include "fawnds_proxy.h"
#include "rate_limiter.h"
#include <pthread.h>
#include <atomic>
#include <unistd.h>

namespace fawn {

    // configuration
    //   <type>: "monitor" (fixed)
    //   <(others)>: used by the main store

    class FawnDS_Monitor : public FawnDS_Proxy {
    public:
        FawnDS_Monitor();
        virtual ~FawnDS_Monitor();

        virtual FawnDS_Return Create();
        virtual FawnDS_Return Open();

        virtual FawnDS_Return Close();

        virtual FawnDS_Return Put(const ConstValue& key, const ConstValue& data);
        virtual FawnDS_Return Append(Value& key, const ConstValue& data);

        virtual FawnDS_Return Delete(const ConstValue& key);

        virtual FawnDS_Return Contains(const ConstValue& key) const;
        virtual FawnDS_Return Length(const ConstValue& key, size_t& len) const;
        virtual FawnDS_Return Get(const ConstValue& key, Value& data, size_t offset = 0, size_t len = -1) const;

    protected:
        static void* thread_main(void* arg);

    private:
        pthread_t tid_;

        volatile bool exiting_;

        mutable std::atomic<size_t> write_ops_;
        mutable std::atomic<size_t> read_ops_;

        uint64_t last_time_;

        RateLimiter rate_limiter_;
    };

} // namespace fawn

#endif  // #ifndef _FAWNDS_MONITOR_H_
