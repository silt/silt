/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _STATUTIL_H_
#define _STATUTIL_H_

//#define MAX_BUCKETS 1000


using namespace std;
namespace fawn {
    class DataStat {
    public:
        DataStat(const char* p, double v_min, double v_max, 
                 double step_linear, 
                 double start_log, double step_log);
        ~DataStat();
        /* insert a new value x */
        void insert(double x);


        size_t index_linear(double x);
        size_t index_log(double x);


        /* return the value of given percentile */
        double percentile(double pct);
        /* return the cumulative probability of value x */
        double cumulative(double x);
        /* return the max value */
        double max() {return x_max;}
        /* return the min value */
        double min() {return x_min;}
        /* return the sum of all values */
        double sum() {return x_sum;}
        /* return the mean value */
        double mean() {return x_sum/x_num;}
        /* return the num of values tracked */
        size_t num()  {return x_num;}
        /* return the median value, i.e. 50% percentile*/
        double median() {return percentile(0.5);}
        /* print a brief summary of this data series */
        void summary();
        /* print the cdf of this data series */
        void cdf(double intv, bool logscale);


    private:
        char name[256];
        double v_min, v_max; // max and min of possible values
        double x_min, x_max; // max and min of input data
        double x_sum;        // sum of all input data
        size_t x_num;      // num of all input data

        size_t num_linear;
        size_t num_log;
        size_t *count_linear;
        size_t *count_log;
        double step_linear;
        double start_log;
        double step_log;
    };
}
#endif /* _STATUTIL_H_ */
