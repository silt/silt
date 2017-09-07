#ifndef __BLOOM_FILTER_H__
#define __BLOOM_FILTER_H__

#include<stdlib.h>

typedef unsigned int (*hashfunc_t)(const char *);
typedef struct {
    size_t asize;
    unsigned char *a;
    size_t nfuncs;
    hashfunc_t *funcs;
} BLOOM;

BLOOM *bloom_create(size_t size, size_t nfuncs, ...);
int bloom_destroy(BLOOM *bloom);
int bloom_add(BLOOM *bloom, const char *s);
int bloom_check(BLOOM *bloom, const char *s);

#endif
