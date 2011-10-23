/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _HASH_FUNCTIONS_H_
#define _HASH_FUNCTIONS_H_

#include "basic_types.h"

typedef uint32_t (*hashfn_t)(const void*, size_t);

namespace fawn {

    class Hashes {
    public:
        static const size_t HASH_COUNT = 3;

        static uint32_t h1(const void* buf, size_t len);
        static uint32_t h2(const void* buf, size_t len);
        static uint32_t h3(const void* buf, size_t len);
        static uint32_t nullhash1(const void* buf, size_t len);
        static uint32_t nullhash2(const void* buf, size_t len);
        static uint32_t nullhash3(const void* buf, size_t len);
        static hashfn_t hashes[HASH_COUNT];
    private:
        Hashes();
    };

}  // namespace fawn

#endif  // #ifndef _HASH_FUNCTIONS_H_
