#pragma once

#include "common.hpp"

namespace cindex
{
	class system_stopwatch
	{
	public:
		void start();
		void stop();

		uint64_t real_time() const CINDEX_WARN_UNUSED_RESULT { return rtime_; }
		uint64_t user_time() const CINDEX_WARN_UNUSED_RESULT { return utime_; }
		uint64_t sys_time() const CINDEX_WARN_UNUSED_RESULT { return stime_; }

	private:
		struct timeval start_rtime_;
		struct timeval start_utime_;
		struct timeval start_stime_;
		uint64_t rtime_;
		uint64_t utime_;
		uint64_t stime_;
	};

	class clock_stopwatch
	{
	public:
		void start();
		void stop();

		uint64_t real_time() const CINDEX_WARN_UNUSED_RESULT { return rtime_; }
		uint64_t user_time() const CINDEX_WARN_UNUSED_RESULT { return utime_; }

	private:
		struct timespec start_rtime_;
		struct timespec start_utime_;
		uint64_t rtime_;
		uint64_t utime_;
	};

	typedef clock_stopwatch stopwatch;

	class scoped_stopwatch
	{
		scoped_stopwatch(uint64_t& t)
			: t_(t), start_rtime_(rdtsc())
		{
		}
		~scoped_stopwatch()
		{
			t_ += rdtsc() - start_rtime_;
		}
		static uint64_t rdtsc()
		{
			unsigned int hi, lo;
			__asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
			return static_cast<uint64_t>(lo) | (static_cast<uint64_t>(hi) << 32);
		}
		uint64_t& t_;
		const uint64_t start_rtime_;
	};
}

