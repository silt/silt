/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "global_limits.h"

#include <cstdio>

namespace fawn {

    GlobalLimits GlobalLimits::global_limits_;

    GlobalLimits::GlobalLimits()
        : convert_rate_limiter_(0, 1000000000L, 1, 1),
        merge_rate_limiter_(0, 1000000000L, 1, 1)
    {
        disabled_ = 0;
        set_convert_rate(1000000000L);
        set_merge_rate(1000000000L);
    }

    GlobalLimits::~GlobalLimits()
    {
    }

    void
    GlobalLimits::set_convert_rate(int64_t v)
    {
        convert_rate_limiter_.set_tokens(v / 100);
        convert_rate_limiter_.set_max_tokens(v / 100);    // allows burst of 0.01 sec
        convert_rate_limiter_.set_ns_per_interval(1000000000L / v);
    }

    void
    GlobalLimits::set_merge_rate(int64_t v)
    {
        merge_rate_limiter_.set_tokens(v / 100);
        merge_rate_limiter_.set_max_tokens(v / 100);    // allows burst of 0.01 sec
        merge_rate_limiter_.set_ns_per_interval(1000000000L / v);
    }

    void
    GlobalLimits::remove_convert_tokens(int64_t v)
    {
        if (enabled()) {
            convert_rate_limiter_.remove_tokens(v);
            //fprintf(stderr, "convert tokens: %lld\n", convert_rate_limiter_.tokens());
        }
    }

    void
    GlobalLimits::remove_merge_tokens(int64_t v)
    {
        if (enabled()) {
            merge_rate_limiter_.remove_tokens(v);
            //fprintf(stderr, "merge tokens: %lld\n", merge_rate_limiter_.tokens());
        }
    }

    void
    GlobalLimits::enable()
    {
        if (--disabled_ < 0) {
            ++disabled_;
            fprintf(stderr, "warning: probably too many calls to GlobalLimits::enable()\n");
        }
    }

    void
    GlobalLimits::disable()
    {
        ++disabled_;
    }

} // namespace fawn
