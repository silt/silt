/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _RATE_LIMITER_H_
#define _RATE_LIMITER_H_

#include <atomic>

namespace fawn {

    // a high-performance high-precision rate limiter based on token bucket

    class RateLimiter {
    public:
        RateLimiter(int64_t initial_tokens, int64_t max_tokens, int64_t new_tokens_per_interval, int64_t ns_per_interval, int64_t min_retry_interval_ns = 1000L);

        void remove_tokens(int64_t v);
        bool try_remove_tokens(int64_t v);

        void remove_tokens_nowait(int64_t v) { tokens_ -= v; }

        void add_tokens(int64_t v) { tokens_ += v; }

        int64_t tokens() const { return tokens_; }
        void set_tokens(int64_t v) { tokens_ = v; }

        const int64_t& max_tokens() const { return max_tokens_; }
        void set_max_tokens(int64_t v) { max_tokens_ = v; }

        const int64_t& new_tokens_per_interval() const { return new_tokens_per_interval_; }
        void set_new_tokens_per_interval(int64_t v) { new_tokens_per_interval_ = v; }

        const int64_t& ns_per_interval() const { return ns_per_interval_; }
        void set_ns_per_interval(int64_t v) { ns_per_interval_ = v; }

        const int64_t& min_retry_interval_ns() const { return min_retry_interval_ns_; }
        void set_min_retry_interval_ns(int64_t v) { min_retry_interval_ns_ = v; }

    protected:
        void update_tokens();

    private:
        std::atomic<int64_t> tokens_;
        int64_t max_tokens_;
        int64_t new_tokens_per_interval_;
        int64_t ns_per_interval_;
        int64_t min_retry_interval_ns_;

        std::atomic<int64_t> last_time_;
    };

} // namespace fawn

#endif  // #ifndef _RATE_LIMITER_H_
