/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <string>
#include <cstdio>
#include <cmath>
#include <cassert>
#include <cstring>

#include "datastat.h"
namespace fawn {
    DataStat::DataStat(const char* p, double v_min, double v_max, 
                       double step_linear = 1.0f, 
                       double start_log = 1.0f,
                       double step_log = 10.0f)
        : v_min(v_min), v_max(v_max), 
          step_linear(step_linear), start_log(start_log), step_log(step_log),
          x_min(v_max), x_max(v_min), x_num(0), x_sum(0.0f)
    {
        assert(v_min <= v_max);
        assert(step_linear > 0);
        assert(start_log > 0);
        assert(step_log > 1);

        memset(name, 0, 256);
        strncpy(name, p, 256);
        
        num_linear = (size_t) ceil((v_max - v_min) / step_linear);
        num_log = (size_t) 1 + ceil( log(v_max / start_log) / log(step_log));
        count_linear = new size_t[num_linear + 1];
        count_log = new size_t[num_log + 1];
        memset(count_linear, 0, sizeof(size_t) * num_linear);
        memset(count_log,    0, sizeof(size_t) * num_log);
    }

    DataStat::~DataStat() 
    {
    }

    
    size_t DataStat::index_linear(double x) {
        size_t i;
        i = (size_t) (x - v_min) / step_linear;
        i = ( i > 0 ) ? i : 0;
        i = ( i < num_linear ) ? i : num_linear - 1;
        return i;
    }

    size_t DataStat::index_log(double x) {
        size_t i;
        if (x < start_log) {
            i = 0;
        } else {
            i = (size_t) 1 + log(x / start_log) / log(step_log);
        }
        i = ( i > 0 ) ? i : 0;
        i = ( i < num_log ) ? i : num_log - 1;
        return i;
    }

    void DataStat::insert(double x) {
        x_num ++;
        x_sum += x;
        x_max = (x_max > x ) ? x_max : x;
        x_min = (x_min < x ) ? x_min : x;
        count_linear[index_linear(x)] ++;
        count_log[index_log(x)] ++;
    }

    double DataStat::percentile(double pct) {
        int i;
        double th = x_num * pct;
        size_t cur;
        double ret_linear  = v_min - 1;
        cur = 0;
        for (i = 0; i < num_linear; i++) {
            cur += count_linear[i];
            if (cur >= th ) {
                ret_linear = v_min + i * step_linear;
                break;
            } 
        }
            
        double ret_log  = v_min - 1;
        cur = 0;
        for (i = 0; i < num_log; i++) {
            cur += count_log[i];
            if (cur >= th ) {
                if ( i == 0 )
                    ret_log = v_min;
                else 
                    ret_log = start_log * pow(step_log, i - 1);
                break;
            } 
        }

        return std::max(ret_linear, ret_log);
    }

    double DataStat::cumulative(double x) {
        double cum_linear = 0;
        for (int i = 0; i < index_linear(x); i++) 
            cum_linear += count_linear[i];
        cum_linear = cum_linear * 1.0 / num();

        double cum_log = 0;
        for (int i = 0; i < index_log(x); i++) 
            cum_log += count_log[i];
        cum_log = cum_log * 1.0 / num();
        
        return std::max(cum_linear, cum_log);
    }
    
    void DataStat::summary() {
        printf("========================================\n");
        printf("\tsummary of %s\n", name);
        printf("----------------------------------------\n");
        if (x_num) {
            printf("Number %llu\n", static_cast<unsigned long long>(num()));
            printf("Average %f\n",  mean());
            printf("Median %f\n",   percentile(0.5));
            printf("Min %f\n",      x_min);
            printf("95%%Min %f\n",  percentile(0.05));
            printf("99%%Min %f\n",  percentile(0.01));
            printf("99.9%%Min %f\n",  percentile(0.001));
            printf("Max %f\n",      x_max);
            printf("95%%Max %f\n",  percentile(0.95));
            printf("99%%Max %f\n",  percentile(0.99));
            printf("99.9%%Max %f\n",  percentile(0.999));
        } else
            printf("N/A\n");
        printf("========================================\n");
    }

    void DataStat::cdf(double intv, bool logscale = false) {
        if (logscale)
            assert(intv > 1);
        printf("========================================\n");
        printf("\tCDF of %s\n", name);
        printf("----------------------------------------\n");
        if (num() == 0) {
            printf("N/A\n");
            printf("========================================\n");
            return;
        } 

        double x;
        if (logscale) {
            x = start_log;
        }
        else {
            x = v_min;
        }
        while (x < v_max) {
            printf("%f\t%f\n", x, cumulative(x));
            if (logscale)
                x *= intv;
            else
                x += intv;
        }

        printf("========================================\n");
    }
}
