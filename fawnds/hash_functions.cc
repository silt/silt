/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "hash_functions.h"
#include "hashutil.h"

namespace fawn {

    hashfn_t Hashes::hashes[HASH_COUNT] = { &Hashes::h1,
                                            &Hashes::h2,
                                            &Hashes::h3 };

    uint32_t Hashes::h1(const void* buf, size_t len)
    {
        return HashUtil::BobHash(buf, len, 0xdeadbeef);
    }
    uint32_t Hashes::h2(const void* buf, size_t len)
    {
        return HashUtil::BobHash(buf, len, 0xc0ffee);
    }
    uint32_t Hashes::h3(const void* buf, size_t len)
    {
        return HashUtil::BobHash(buf, len, 0x101CA75);
    }

    // These hashes assume that len > 32bits
    uint32_t Hashes::nullhash1(const void* buf, size_t len)
    {
        if (len < 4) printf("Warning: keylength is not long enough to use null hash 1.  Ensure keylength >= 32bits\n");
        return HashUtil::NullHash(buf, len, 0);
    }

    uint32_t Hashes::nullhash2(const void* buf, size_t len)
    {
        if (len < 8) printf("Warning: keylength is not long enough to use null hash 2.  Ensure keylength >= 64bits\n");
        return HashUtil::NullHash(buf, len, 4);
    }

    uint32_t Hashes::nullhash3(const void* buf, size_t len)
    {
        if (len < 12) printf("Warning: keylength is not long enough to use null hash 3.  Ensure keylength >= 96bits\n");
        return HashUtil::NullHash(buf, len, 8);
    }

}  // namespace fawn
