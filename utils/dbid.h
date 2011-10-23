/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _DBID_H_
#define _DBID_H_

using namespace std;
#include <iostream>
#include <cstdlib>
#include <stdint.h>

/**
 * DBID is a fixed-length identifier for nodes on a ringspace.  Since
 * it is fixed length, the length needs to be as long as the longest
 * allowable key size so that the ID space contains the granularity to
 * load balance data.  This length is defined as DBID_LENGTH below.
 *
 * In other words: DBID_LENGTH is the maximum key length for any query.
 *
 * Comparisons of short keys will pad the incoming key with 0s to
 * meet this size when necessary.  You can redefine DBID_LENGTH to
 * whatever size you desire, but DBID comparisons will take time
 * proportional to the size.
 **/

namespace fawn {
    const uint32_t DBID_LENGTH = 1024;

    class DBID
    {

    private:
        char value[DBID_LENGTH];
        unsigned int actual_size;
    public:
        DBID();
        DBID(const char *c, unsigned int size);
        explicit DBID(const string &s);
        ~DBID();
        void Init(const char *c, unsigned int size);

        char* data() {
            return value;
        }
        const char *const_data() const {
            return (const char *)value;
        }
        string* actual_data();
        const string actual_data_str() const;
        uint32_t size() {
            return DBID_LENGTH;
        }
        unsigned int get_actual_size() {
            return actual_size;
        }
        void printValue() const;
        void printValue(int prefix_bytes) const;
        void set_actual_size(unsigned int as) {
            actual_size = as;
        }

        bool operator==(const DBID &rhs) const;
        bool operator<(const DBID &rhs) const;

        // This compares only the top 64 bits of each DBID
        uint64_t operator-(const DBID &rhs) const;

        //DBID operator-(const DBID &rhs) const;
    };

    bool between(const string& startKey, const string& endKey, const string& thisKey);
    bool between(const DBID* startID, const DBID* endID, const DBID* thisID);
}
#endif
