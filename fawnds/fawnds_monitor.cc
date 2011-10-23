/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "fawnds_monitor.h"
#include <stdio.h>
#include <sys/time.h>
#ifndef __APPLE__
#include <sys/sysinfo.h>
#endif

namespace fawn {

    FawnDS_Monitor::FawnDS_Monitor()
        : exiting_(false),
        rate_limiter_(1, 1, 1, 1000000000L)    // 1s report interval
    {
        write_ops_ = 0;
        read_ops_ = 0;
    }

    FawnDS_Monitor::~FawnDS_Monitor()
    {
        // dup -- FawnDS_Proxy calls its Close() in destructor
        if (store_)
            Close();
    }

    FawnDS_Return
    FawnDS_Monitor::Create()
    {
        FawnDS_Return ret = this->FawnDS_Proxy::Create();
        if (store_) {
            struct timeval tv;
            if (gettimeofday(&tv, NULL)) {
                perror("Error while getting the current time");
            }
            last_time_ = static_cast<uint64_t>(tv.tv_sec) * 1000000000Lu + static_cast<uint64_t>(tv.tv_usec) * 1000Lu;

            if (pthread_create(&tid_, NULL, thread_main, this)) {
                perror("Error while creating a monitor thread");
            }
        }
        return ret;
    }

    FawnDS_Return
    FawnDS_Monitor::Open()
    {
        FawnDS_Return ret = this->FawnDS_Proxy::Open();
        if (store_) {
            struct timeval tv;
            if (gettimeofday(&tv, NULL)) {
                perror("Error while getting the current time");
            }
            last_time_ = static_cast<uint64_t>(tv.tv_sec) * 1000000000Lu + static_cast<uint64_t>(tv.tv_usec) * 1000Lu;

            if (pthread_create(&tid_, NULL, thread_main, this)) {
                perror("Error while creating a monitor thread");
            }
        }
        return ret;
    }

    FawnDS_Return
    FawnDS_Monitor::Close()
    {
        if (store_) {
            // this has a race condition issue but should be find if usually only one thread calls Close()
            exiting_ = true;
            //if (pthread_cancel(tid_)) {
            //    perror("Error while canceling the monitor thread");
            //}
            if (pthread_join(tid_, NULL)) {
                perror("Error while stopping the monitor thread");
            }
        }

        return this->FawnDS_Proxy::Close();
    }


    FawnDS_Return
    FawnDS_Monitor::Put(const ConstValue& key, const ConstValue& data)
    {
        ++write_ops_;
        return this->FawnDS_Proxy::Put(key, data);
    }

    FawnDS_Return
    FawnDS_Monitor::Append(Value& key, const ConstValue& data)
    {
        ++write_ops_;
        return this->FawnDS_Proxy::Append(key, data);
    }

    FawnDS_Return
    FawnDS_Monitor::Delete(const ConstValue& key)
    {
        ++write_ops_;
        return this->FawnDS_Proxy::Delete(key);
    }

    FawnDS_Return
    FawnDS_Monitor::Contains(const ConstValue& key) const
    {
        ++read_ops_;
        return this->FawnDS_Proxy::Contains(key);
    }

    FawnDS_Return
    FawnDS_Monitor::Length(const ConstValue& key, size_t& len) const
    {
        ++read_ops_;
        return this->FawnDS_Proxy::Length(key, len);
    }

    FawnDS_Return
    FawnDS_Monitor::Get(const ConstValue& key, Value& data, size_t offset, size_t len) const
    {
        ++read_ops_;
        return this->FawnDS_Proxy::Get(key, data, offset, len);
    }

    void*
    FawnDS_Monitor::thread_main(void* arg)
    {
        FawnDS_Monitor* monitor = reinterpret_cast<FawnDS_Monitor*>(arg);

        while (!monitor->exiting_) {
            monitor->rate_limiter_.remove_tokens(1);

            size_t read_ops = monitor->read_ops_;
            monitor->read_ops_ -= read_ops;

            size_t write_ops = monitor->write_ops_;
            monitor->write_ops_ -= write_ops;

            struct timeval tv;
            if (gettimeofday(&tv, NULL)) {
                perror("Error while getting the current time");
            }
            
            uint64_t current_time = static_cast<uint64_t>(tv.tv_sec) * 1000000000Lu + static_cast<uint64_t>(tv.tv_usec) * 1000Lu;

            double time_diff = static_cast<double>(current_time - monitor->last_time_) / 1000000000.;
            monitor->last_time_ = current_time;

            size_t rmem_size = 0;
#ifndef __APPLE__
            FILE* fp = fopen("/proc/self/statm", "r");
            if (fp) {
                unsigned long vmem;
                unsigned long rmem = 0;
                if (fscanf(fp, "%lu %lu", &vmem, &rmem) == 2) {
                    rmem_size = rmem * getpagesize();
                }
                fclose(fp);
            }
#endif

            Value status_num_active_data;
            monitor->Status(NUM_ACTIVE_DATA, status_num_active_data);

            Value status_memory_use;
            monitor->Status(MEMORY_USE, status_memory_use);

            fprintf(stdout, "%llu.%06llu: %9.2lf reads/s, %9.2lf writes/s, %7.2lf MB total\n",
                    static_cast<long long unsigned>(tv.tv_sec),
                    static_cast<long long unsigned>(tv.tv_usec),
                    static_cast<double>(read_ops) / time_diff,
                    static_cast<double>(write_ops) / time_diff,
                    static_cast<double>(rmem_size) / 1000000.
                );
            fprintf(stdout, "%llu.%06llu: NUM_ACTIVE_DATA %s\n",
                    static_cast<long long unsigned>(tv.tv_sec),
                    static_cast<long long unsigned>(tv.tv_usec),
                    status_num_active_data.str().c_str()
                );
            fprintf(stdout, "%llu.%06llu: MEMORY_USE %s\n",
                    static_cast<long long unsigned>(tv.tv_sec),
                    static_cast<long long unsigned>(tv.tv_usec),
                    status_memory_use.str().c_str()
                );
            fflush(stdout);
       }

        return NULL;
    }

} // namespace fawn
