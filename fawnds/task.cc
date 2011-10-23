/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "task.h"
#include "debug.h"
#include <cassert>

#include <stdio.h>
#include <errno.h>

#ifdef __linux__

#include <sys/types.h>
#include <bits/syscall.h>

extern "C" {
pid_t gettid()
{
    return (pid_t)syscall(SYS_gettid);
}
}

extern "C" {
// from linux/ioprio.h

#define IOPRIO_BITS             (16)
#define IOPRIO_CLASS_SHIFT      (13)
#define IOPRIO_PRIO_MASK        ((1UL << IOPRIO_CLASS_SHIFT) - 1)

#define IOPRIO_PRIO_CLASS(mask) ((mask) >> IOPRIO_CLASS_SHIFT)
#define IOPRIO_PRIO_DATA(mask)  ((mask) & IOPRIO_PRIO_MASK)
#define IOPRIO_PRIO_VALUE(class, data)  (((class) << IOPRIO_CLASS_SHIFT) | data)

#define ioprio_valid(mask)      (IOPRIO_PRIO_CLASS((mask)) != IOPRIO_CLASS_NONE)

enum {
    IOPRIO_CLASS_NONE,
    IOPRIO_CLASS_RT,
    IOPRIO_CLASS_BE,
    IOPRIO_CLASS_IDLE,
};

enum {
    IOPRIO_WHO_PROCESS = 1,
    IOPRIO_WHO_PGRP,
    IOPRIO_WHO_USER,
};

static inline int ioprio_set(int which, int who, int ioprio)
{
    return syscall(__NR_ioprio_set, which, who, ioprio);
}

static inline int ioprio_get(int which, int who)
{
    return syscall(__NR_ioprio_get, which, who);
}

}
#endif


namespace fawn {
    TaskScheduler::TaskScheduler(size_t num_workers, size_t queue_capacity, cpu_priority_t cpu_priority, io_priority_t io_priority)
        : cpu_priority_(cpu_priority), io_priority_(io_priority)
    {
        assert(num_workers > 0);

        shared_queue_.set_capacity(queue_capacity);

        for (size_t i = 0; i < num_workers; i++) {
            Worker* worker = new Worker();
            worker->owner = this;
            worker->seen_join = false;
            worker->queue = &shared_queue_;
            //worker->queue.set_capacity(queue_capacity);

            if (pthread_create(&worker->tid, NULL, Worker::thread_main, worker) != 0) {
                DPRINTF(2, "TaskScheduler::TaskScheduler(): failed to create worker\n");
                break;
            }

            workers_.push_back(worker);
        }

        //next_worker_ = 0;
    }

    TaskScheduler::~TaskScheduler()
    {
        if (workers_.size())
            join();
    }

    void
    TaskScheduler::enqueue_task(Task* t)
    {
        //workers_[next_worker_++ % workers_.size()]->queue.push(t);
        shared_queue_.push(t);
    }

    void
    TaskScheduler::join()
    {
        for (size_t i = 0; i < workers_.size(); i++) {
            JoinTask* t = new JoinTask();
            //workers_[i]->queue.push(t);
            shared_queue_.push(t);
        }

        for (size_t i = 0; i < workers_.size(); i++) {
            pthread_join(workers_[i]->tid, NULL);
            delete workers_[i];
            workers_[i] = NULL;
        }

        workers_.clear();
    }

    void*
    TaskScheduler::Worker::thread_main(void* arg)
    {
        Worker* this_worker = reinterpret_cast<Worker*>(arg);

#ifdef __linux__
        pid_t tid = gettid();

        {
            if (this_worker->owner->cpu_priority_ == CPU_LOW) {
                int new_nice = nice(1);
                //fprintf(stderr, "set nice() = %d\n", new_nice);
            }
        }

        {
            // from Documentation/block/ioprio.txt
            int ioprio = 4;
            int ioprio_class = IOPRIO_CLASS_BE;

            switch (this_worker->owner->io_priority_) {
            case IO_REAL_TIME:
                ioprio_class = IOPRIO_CLASS_RT;
                break;
            case IO_BEST_EFFORT:
                ioprio_class = IOPRIO_CLASS_BE;
                break;
            case IO_IDLE:
                ioprio_class = IOPRIO_CLASS_IDLE;
                ioprio = 7;
                break;
            default:
                assert(false);
                break;
            }

            int ret = ioprio_set(IOPRIO_WHO_PROCESS, tid, IOPRIO_PRIO_VALUE(ioprio_class, ioprio));
            if (ret) {
                perror("Error while setting I/O priority");
                assert(false);
            }
            assert(IOPRIO_PRIO_CLASS(ioprio_get(IOPRIO_WHO_PROCESS, tid)) == ioprio_class);
            assert(IOPRIO_PRIO_DATA(ioprio_get(IOPRIO_WHO_PROCESS, tid)) == ioprio);
        }
#endif

        while (!this_worker->seen_join) {
            Task* t;
            this_worker->queue->pop(t);
            t->seen_join = &this_worker->seen_join;
            t->Run();
            delete t;
        }
        return NULL;
    }

    void
    TaskScheduler::JoinTask::Run()
    {
        *seen_join = true;
    }

} // namespace fawn
