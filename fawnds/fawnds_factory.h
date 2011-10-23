/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNDS_FACTORY_H_
#define _FAWNDS_FACTORY_H_

//using fawn::DataHeader;
//using fawn::HashUtil;

//using namespace std;

#include "configuration.h"
#include "fawnds.h"

namespace fawn {

    class FawnDS_Factory {
    public:
        static FawnDS* New(std::string config_file);
        static FawnDS* New(const Configuration* config);
    };

} // namespace fawn

#endif  // #ifndef _FAWNDS_FACTORY_H_
