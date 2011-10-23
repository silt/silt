/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNDS_TYPES_H_
#define _FAWNDS_TYPES_H_

namespace fawn {

    enum FawnDS_Return {
        OK = 0,
        ERROR,
        UNSUPPORTED,
        KEY_DELETED,
        KEY_NOT_FOUND,
        INVALID_KEY,
        INVALID_DATA,
        INVALID_LENGTH,
        INSUFFICIENT_SPACE,
        END,
    };

    enum FawnDS_StatusType {
        NUM_DATA = 0,       // returns # of total data (including any dummy entries & holes)
        NUM_ACTIVE_DATA,    // returns # of total active (valid) data
        CAPACITY,           // returns # of total data the data store can possibly hold
        MEMORY_USE,         // returns memory byte size
        DISK_USE,           // returns disk byte size
    };

} // namespace fawn

#endif  // #ifndef _FAWNDS_TYPES_H_
