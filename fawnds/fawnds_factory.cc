/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "fawnds_factory.h"
#include "hash_table_default.h"
#include "hash_table_cuckoo.h"
#include "file_store.h"
#include "fawnds.h"
#include "fawnds_sf.h"
#include "fawnds_sf_ordered_trie.h"
#include "fawnds_partition.h"
#include "fawnds_combi.h"
#include "sorter.h"
#include "fawnds_proxy.h"
#include "fawnds_monitor.h"
#include "bdb.h"
#include "debug.h"

namespace fawn {

    FawnDS*
    FawnDS_Factory::New(std::string config_file)
    {
        DPRINTF(2, "FawnDS_Factory::New(): use config file: %s\n", config_file.c_str());
        Configuration* config = new Configuration(config_file);
        return FawnDS_Factory::New(config);
    }

    FawnDS*
    FawnDS_Factory::New(const Configuration* config)
    {
        std::string type = config->GetStringValue("child::type");
        DPRINTF(2, "FawnDS_Factory::New(): creating a new instance of type=%s\n", type.c_str());
        FawnDS* result = NULL;

        if (type == "file")
            result = new FileStore();
        else if (type == "sf")
            result = new FawnDS_SF();
        else if (type == "sf_ordered_trie")
            result = new FawnDS_SF_Ordered_Trie();
        else if (type == "partition")
            result = new FawnDS_Partition();
        else if (type == "combi")
            result = new FawnDS_Combi();
        else if (type == "default")
            result = new HashTableDefault();
        else if (type == "cuckoo")
            result = new HashTableCuckoo();
        else if (type == "sorter")
            result = new Sorter();
        else if (type == "proxy")
            result = new FawnDS_Proxy();
        else if (type == "monitor")
            result = new FawnDS_Monitor();
#ifdef HAVE_LIBDB
        else if (type == "bdb")
            result = new BDB();
#endif

        if (!result)
            return NULL;

        result->SetConfig(config);
        return result;
    }

} // namespace fawn
