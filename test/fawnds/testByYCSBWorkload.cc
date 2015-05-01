/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <time.h>
#include <inttypes.h>
#include <pthread.h>

#include "fawnds_factory.h"
#include "rate_limiter.h"
#include "global_limits.h"
#include "datastat.h"

#include "preprocessTrace.h"

using namespace std;
using namespace tbb;
using namespace fawn;

#define MAX_QUERIES 1048576 

int32_t num_threads = 1;
int64_t max_ops_per_sec = 1000000000L;
int64_t convert_rate = 1000000000L;
int64_t merge_rate = 1000000000L;
double successful_get_ratio = 1.;

FawnDS *h;
size_t val_len;

RateLimiter *rate_limiter;

struct Query q_buf[MAX_QUERIES];
uint64_t num_query_sent = 0;
uint64_t num_query_read = 0;
bool done;

pthread_mutex_t query_lock;
pthread_mutex_t stat_lock;

DataStat *latency_put;
DataStat *latency_get;


void *query_reader(void *p) {
    FILE *fp = (FILE *) p;
    size_t n; 
                
    printf("starting thread: query_reader!\n");                

    if (fread(&val_len, sizeof(size_t), 1, fp))
        cout << "val_len=" << val_len << "bytes" <<  endl;
    else
        exit(-1);

    while(!feof((FILE*) fp)) {
        if (num_query_read < num_query_sent + MAX_QUERIES/2) {
            n = fread(&(q_buf[num_query_read % MAX_QUERIES]), sizeof(Query), 1024,  fp);
             
            num_query_read = num_query_read + n;
            
            //printf("[query_reader] num_query_read=%ld\n", num_query_read);
        } else {
            //cout << "query_reader is sleeping! num_query_read=" << num_query_read << " num_query_sent=" << num_query_sent << endl;
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 100 * 1000;
            nanosleep(&ts, NULL);
        }
    }
    done = true;
    fclose(fp);
    printf("killing thread: query_reader!\n");                
    pthread_exit(NULL);
}

void *query_sender(void * id) {
    struct Query q;
    FawnDS_Return ret;
    long t = (long) id;
    char val[(MAXLEN + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t)];
    unsigned int data_seed = 0;
    struct timespec ts;
    double last_time_, current_time_, latency;
    printf("starting thread: query_sender%ld!\n", t);
    while (1) {

        pthread_mutex_lock(&query_lock);

        if (num_query_sent < num_query_read) {
            uint64_t cur = num_query_sent;
            num_query_sent ++;
            q = q_buf[cur % MAX_QUERIES];   // copy data to ensure no stall access (with some concurrency penalty)

            pthread_mutex_unlock(&query_lock);

            rate_limiter->remove_tokens(1);
            if (q.tp == PUT) {
                uint64_t r = (static_cast<uint64_t>(rand_r(&data_seed)) << 32) | static_cast<uint64_t>(rand_r(&data_seed));
                uint64_t* p = reinterpret_cast<uint64_t*>(val);
                const uint64_t* p_end = p + (val_len / sizeof(uint64_t));
                while (p != p_end)
                    *p++ = r++;

                clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
                last_time_ = static_cast<int64_t>(ts.tv_sec) * 1000000000Lu + static_cast<int64_t>(ts.tv_nsec);
                ret = h->Put(ConstRefValue(q.hashed_key, 20), 
                             ConstRefValue(val, val_len));         
                if (ret != OK) {
                    printf("error! h->Put() return value=%d, expected=%d, operation%llu\n", 
                           ret, OK, static_cast<unsigned long long>(cur));
                    //exit(1);
                }
                clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
                current_time_ = static_cast<int64_t>(ts.tv_sec) * 1000000000Lu + static_cast<int64_t>(ts.tv_nsec);
                latency = current_time_ - last_time_;

                pthread_mutex_lock(&stat_lock);
                latency_put->insert(latency);
                pthread_mutex_unlock(&stat_lock);



            } else if (q.tp == GET) {
                FawnDS_Return expected_ret;
                if (static_cast<double>(rand()) / static_cast<double>(RAND_MAX) <= successful_get_ratio)
                    expected_ret = OK;
                else {
                    // make the query to be unsuccessful assuming extremely low key collision
                    for (size_t i = 0; i < 20; i += 2)
                        *reinterpret_cast<uint16_t*>(q.hashed_key + i) = static_cast<uint16_t>(rand());
                    expected_ret = KEY_NOT_FOUND;
                }

                SizedValue<MAXLEN> read_data;
                clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
                last_time_ = static_cast<int64_t>(ts.tv_sec) * 1000000000Lu + static_cast<int64_t>(ts.tv_nsec);                
                ret = h->Get(ConstRefValue(q.hashed_key, 20), read_data);
                if (ret != expected_ret) {
                    printf("error! h->Get() return value=%d, expected=%d, operation%llu\n", 
                           ret, expected_ret, static_cast<unsigned long long>(cur));
                    //exit(1);
                }

                clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
                current_time_ = static_cast<int64_t>(ts.tv_sec) * 1000000000Lu + static_cast<int64_t>(ts.tv_nsec);                
                latency = current_time_ - last_time_;

                pthread_mutex_lock(&stat_lock);
                latency_get->insert(latency);
                pthread_mutex_unlock(&stat_lock);

            } else {
                printf("unknown query type %d.\n", q.tp);
            }

        } else {
            pthread_mutex_unlock(&query_lock);

            if (done) {
                break;
            }
            else {
                //cout << "query_sender is sleeping! num_query_read=" << num_query_read << " num_query_sent=" << num_query_sent << endl;
                struct timespec ts;
                ts.tv_sec = 0;
                ts.tv_nsec = 100 * 1000;
                nanosleep(&ts, NULL);
            }
        }
    } /* end of while (done) */
    printf("killing thread: query_sender%ld!\n", t);
    pthread_exit(NULL);
} /* end of query_sender */


void replay(string recfile) {
    pthread_t reader_thread;
    pthread_t *sender_threads = new pthread_t[num_threads];

    // see if the recfile is good
    FILE *fp = fopen(recfile.c_str(), "r");
    if (fp == NULL) {
        cout << "can not open file " << recfile << endl;
        exit(-1);
    }

    pthread_mutex_init(&query_lock, 0 );
    pthread_mutex_init(&stat_lock, 0 );
    num_query_read = num_query_sent = 0;
    done = false;

    latency_put = new DataStat("PUT-LATENCY(ns)", 0, 100000000, 10000., 10., 1.02);
    latency_get = new DataStat("GET-LATENCY(ns)", 0, 100000000, 10000., 10., 1.02);

    // create threads!
    pthread_create(&reader_thread, NULL, query_reader, (void *) fp);
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 1000 * 1000;
    nanosleep(&ts, NULL);
    for (int t = 0; t < num_threads; t++) {
	long t2 = (long)t;
        pthread_create(&sender_threads[t], NULL, query_sender, (void *) t2);
    }

    // wait for threads 
    pthread_join(reader_thread, NULL);
    for (int t = 0; t < num_threads; t++) {
        pthread_join(sender_threads[t], NULL);
    }

    // output and destroy everything
    printf("total %llu ops in %f sec: tput = %.2f KQPS\n", 
           static_cast<unsigned long long> (latency_put->num() + latency_get->num()), 
           0.000000001 * (latency_put->sum() + latency_get->sum()),
           1000000. * (latency_put->num() + latency_get->num()) / (latency_put->sum() + latency_get->sum())        );

    latency_put->summary();
    latency_get->summary();

    if (latency_put->num()) {
        latency_put->cdf(1.02, true);
    }

    if (latency_get->num()) {
        latency_get->cdf(1.02, true);
    }

    pthread_mutex_destroy(&query_lock);
    pthread_mutex_destroy(&stat_lock);
    delete latency_put;
    delete latency_get;

    h->Flush();
    sleep(10);  // these sleep() gives time for FawnDS_Monitor to print status messages after the store reaches a steady state
    sync();
    sleep(5);
}


void usage() {
    cout << "usage: testByYCSBWorkload conf_file load_workload_name trans_workload_name [-t num_threads] [-r max_ops_per_sec] [-c convert_rate] [-m merge_rate] [-s successful_get_ratio]" << endl 
         << "e.g. ./testByYCSBWorkload testConfigs/bdb.xml testWorkloads/update_only" << endl;

}

int main(int argc, char **argv) {

    char ch;
    while ((ch = getopt(argc, argv, "t:r:c:m:s:")) != -1)
        switch (ch) {
        case 't': num_threads = atoi(optarg); break;
        case 'r': max_ops_per_sec = atoll(optarg); break;
        case 'c': convert_rate = atoll(optarg); break;
        case 'm': merge_rate = atoll(optarg); break;
        case 's': successful_get_ratio = atof(optarg); break;
        default:
            usage();
            exit(-1);
        }

    argc -= optind;
    argv += optind;

    if (argc < 3) {
        usage();
        exit(-1);
    }

    cout << "num_threads: " << num_threads << endl;
    cout << "max_ops_per_sec: " << max_ops_per_sec << endl;
    cout << "convert_rate: " << convert_rate << endl;
    cout << "merge_rate: " << merge_rate << endl;
    cout << "successful_get_ratio: " << successful_get_ratio << endl;

    string conf_file = string(argv[0]);
    string load_workload = string(argv[1]);
    string trans_workload = string(argv[2]);

    // clean dirty pages to reduce anomalies
    sync();

    GlobalLimits::instance().set_convert_rate(convert_rate);
    GlobalLimits::instance().set_merge_rate(merge_rate);

    // prepare FAWNDS
    h = FawnDS_Factory::New(conf_file); // test_num_records_
    if (h->Create() != OK) {
        cout << "cannot create FAWNDS!" << endl;
        exit(0);
    }

    rate_limiter = new RateLimiter(0, 1000000000L, 1, 1);   // virtually unlimted
    GlobalLimits::instance().disable();

    cout << "process load ... " << endl;
    replay(load_workload + ".load");
    cout << "process load ... done" << endl;
    cout << endl << endl << endl;

    delete rate_limiter;

    rate_limiter = new RateLimiter(0, max_ops_per_sec, 1, 1000000000L / max_ops_per_sec);    
    GlobalLimits::instance().enable();

    cout << "process transaction ... " << endl;
    replay(trans_workload + ".trans");
    cout << "process transaction ... done" << endl;

    delete rate_limiter;

    h->Close();
    delete h;
}
