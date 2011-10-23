/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _HASHUTIL_H_
#define _HASHUTIL_H_

#include <sys/types.h>
#include <string>
#include <stdlib.h>
#include <stdint.h>
#include <openssl/evp.h>
#include "fnv.h"

using namespace std;

#define rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))
#define mix(a,b,c)		                \
    {						\
	a -= c;  a ^= rot(c, 4);  c += b;	\
	b -= a;  b ^= rot(a, 6);  a += c;	\
	c -= b;  c ^= rot(b, 8);  b += a;	\
	a -= c;  a ^= rot(c,16);  c += b;	\
	b -= a;  b ^= rot(a,19);  a += c;	\
	c -= b;  c ^= rot(b, 4);  b += a;	\
    }

#define final(a,b,c)				\
    {						\
	c ^= b; c -= rot(b,14);			\
	a ^= c; a -= rot(c,11);			\
	b ^= a; b -= rot(a,25);			\
	c ^= b; c -= rot(b,16);			\
	a ^= c; a -= rot(c,4);			\
	b ^= a; b -= rot(a,14);			\
	c ^= b; c -= rot(b,24);			\
    }
    // Assuming little endian
#define HASH_LITTLE_ENDIAN 1

#define get16bits(d) (*((const uint16_t *) (d)))

namespace fawn {
    class HashUtil {
    public:
	// Bob Jenkins Hash
	static uint32_t BobHash(const void *buf, size_t length, uint32_t seed = 0);
	static uint32_t BobHash(const string &s, uint32_t seed = 0);

	// Bob Jenkins Hash that returns two indices in one call
	// Useful for Cuckoo hashing, power of two choices, etc.
	// Use idx1 before idx2, when possible. idx1 and idx2 should be initialized to seeds.
	static void BobHash(const void *buf, size_t length, uint32_t *idx1,  uint32_t *idx2);
	static void BobHash(const string &s, uint32_t *idx1,  uint32_t *idx2);

	// MurmurHash2
	static uint32_t MurmurHash(const void *buf, size_t length, uint32_t seed = 0);
	static uint32_t MurmurHash(const string &s, uint32_t seed = 0);

	// FNV Hash
	static uint32_t FNVHash(const void *buf, size_t length);
	static uint32_t FNVHash(const string &s);

	// SuperFastHash
	static uint32_t SuperFastHash(const void *buf, size_t len);
	static uint32_t SuperFastHash(const string &s);

	// Integer hashes (from Bob Jenkins)
        static uint32_t hashint_full_avalanche_1( uint32_t a);
        static uint32_t hashint_full_avalanche_2( uint32_t a);
        static uint32_t hashint_half_avalanche( uint32_t a);

	// Null hash (shift and mask)
	static uint32_t NullHash(const void* buf, size_t length, uint32_t shiftbytes);

	// Wrappers for MD5 and SHA1 hashing using EVP
	static std::string MD5Hash(const char* inbuf, size_t in_length);
	static std::string SHA1Hash(const char* inbuf, size_t in_length);

        static uint32_t FindNextHashSize(uint32_t number)
        {
            // Gets the next highest power of 2 larger than number
            number--;
            number = (number >> 1) | number;
            number = (number >> 2) | number;
            number = (number >> 4) | number;
            number = (number >> 8) | number;
            number = (number >> 16) | number;
            number++;
            return number;
        }

    private:
	HashUtil();
    };
}

#endif  // #ifndef _HASHUTIL_H_


