/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _TASK_H_
#define _TASK_H_

#include <pthread.h>
#include <vector>
#include <tbb/concurrent_queue.h>

namespace fawn {

    // a simple task scheduler with round-robin per-thread queues; we don't use the TBB task scheduler, which is geared towared computation-oriented tasks

    class Task {
    public:
        virtual ~Task() {}
        virtual void Run() = 0;
        bool* seen_join;
    };

    class TaskScheduler {
    public:
        enum cpu_priority_t {
            CPU_HIGH,
            CPU_NORMAL,
            CPU_LOW,
        };

        enum io_priority_t {
            IO_REAL_TIME,
            IO_BEST_EFFORT,
            IO_IDLE,
        };

        TaskScheduler(size_t num_workers, size_t queue_capacity, cpu_priority_t cpu_priority = CPU_NORMAL, io_priority_t io_priority = IO_BEST_EFFORT);
        ~TaskScheduler();

        void enqueue_task(Task* t);
        void join();

    protected:
        struct Worker {
            TaskScheduler* owner;
            pthread_t tid;
            tbb::concurrent_bounded_queue<Task*>* queue;
            bool seen_join;

            static void* thread_main(void* arg);
        };

        class JoinTask : public Task {
        public:
            virtual void Run();
        };

    private:
        cpu_priority_t cpu_priority_;
        io_priority_t io_priority_;
        std::vector<Worker*> workers_;
        //tbb::atomic<size_t> next_worker_;
        tbb::concurrent_bounded_queue<Task*> shared_queue_;
    };

} // namespace fawn

#endif  // #ifndef _TASK_H_
