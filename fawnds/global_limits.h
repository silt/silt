/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _GLOBAL_LIMITS_H_
#define _GLOBAL_LIMITS_H_

#include "rate_limiter.h"
#include <atomic>

namespace fawn {

    class GlobalLimits {
    public:
        static GlobalLimits& instance() { return global_limits_; }

        void set_convert_rate(int64_t v);
        void set_merge_rate(int64_t v);

        void remove_convert_tokens(int64_t v);
        void remove_merge_tokens(int64_t v);

        bool enabled() const { return disabled_ == 0; }

        void enable();
        void disable();

    protected:
        GlobalLimits();
        ~GlobalLimits();

    private:
        static GlobalLimits global_limits_;

        std::atomic<int> disabled_;

        RateLimiter convert_rate_limiter_;
        RateLimiter merge_rate_limiter_;
    };

} // namespace fawn

#endif  // #ifndef _GLOBAL_LIMITS_H_
