/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _TIMING_H_
#define _TIMING_H_

#include <time.h>
#include <sys/time.h>

#ifdef __cplusplus
extern "C" {
#endif

    double timeval_diff(const struct timeval * const start, const struct timeval * const end);


#ifdef __cplusplus
} /* extern C */
#endif


/* You'll have to change the HZ constant in the printf below... */
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>

#if defined(__i386__)

static __inline__ unsigned long long rdtsc(void)
{
    unsigned long long int x;
    __asm__ volatile (".byte 0x0f, 0x31" : "=A" (x));
    return x;
}
#elif defined(__x86_64__)

static __inline__ unsigned long long rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}
#endif

#endif /* _TIMING_H_ */
