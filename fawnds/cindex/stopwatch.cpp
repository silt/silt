#include "stopwatch.hpp"

#include <sys/time.h>
#include <sys/resource.h>
#include <time.h>

namespace cindex
{
	void
	system_stopwatch::start()
	{
		struct rusage ru;
		gettimeofday(&start_rtime_, NULL);
		getrusage(RUSAGE_SELF, &ru);
		start_utime_ = ru.ru_utime;
		start_stime_ = ru.ru_stime;
	}

	void
	system_stopwatch::stop()
	{
		struct timeval end_rtime;
		struct timeval end_utime;
		struct timeval end_stime;

		struct rusage ru;
		gettimeofday(&end_rtime, NULL);
		getrusage(RUSAGE_SELF, &ru);
		end_utime = ru.ru_utime;
		end_stime = ru.ru_stime;

		rtime_ = (guarded_cast<uint64_t>(end_rtime.tv_sec) * 1000000 + guarded_cast<uint64_t>(end_rtime.tv_usec)) * 1000;
		rtime_ -= (guarded_cast<uint64_t>(start_rtime_.tv_sec) * 1000000 + guarded_cast<uint64_t>(start_rtime_.tv_usec)) * 1000;

		utime_ = (guarded_cast<uint64_t>(end_utime.tv_sec) * 1000000 + guarded_cast<uint64_t>(end_utime.tv_usec)) * 1000;
		utime_ -= (guarded_cast<uint64_t>(start_utime_.tv_sec) * 1000000 + guarded_cast<uint64_t>(start_utime_.tv_usec)) * 1000;

		stime_ = (guarded_cast<uint64_t>(end_stime.tv_sec) * 1000000 + guarded_cast<uint64_t>(end_stime.tv_usec)) * 1000;
		stime_ -= (guarded_cast<uint64_t>(start_stime_.tv_sec) * 1000000 + guarded_cast<uint64_t>(start_stime_.tv_usec)) * 1000;
	}

	void
	clock_stopwatch::start()
	{
		int ret;
		ret = clock_gettime(CLOCK_MONOTONIC_RAW, &start_rtime_);
		assert(!ret);
		ret = clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start_utime_);
		assert(!ret);
	}

	void
	clock_stopwatch::stop()
	{
		struct timespec end_rtime;
		struct timespec end_utime;

		clock_gettime(CLOCK_MONOTONIC_RAW, &end_rtime);
		clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &end_utime);

		rtime_ = guarded_cast<uint64_t>(end_rtime.tv_sec) * 1000000000 + guarded_cast<uint64_t>(end_rtime.tv_nsec);
		rtime_ -= guarded_cast<uint64_t>(start_rtime_.tv_sec) * 1000000000 + guarded_cast<uint64_t>(start_rtime_.tv_nsec);

		utime_ = guarded_cast<uint64_t>(end_utime.tv_sec) * 1000000000 + guarded_cast<uint64_t>(end_utime.tv_nsec);
		utime_ -= guarded_cast<uint64_t>(start_utime_.tv_sec) * 1000000000 + guarded_cast<uint64_t>(start_utime_.tv_nsec);
	}
}

