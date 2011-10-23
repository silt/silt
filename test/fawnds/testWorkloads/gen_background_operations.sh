#!/bin/bash

source common.sh

## for background operation impacts
generate_load exp_100GB 1000 "-P exp_100GB" &
generate_trans exp_100GB_update_perf_10 1000 "-P exp_100GB -P exp_update_perf_10" &

# or smaller workload
#generate_load exp_50GB 1000 "-P exp_50GB" &
#generate_trans exp_50GB_update_perf_10 1000 "-P exp_50GB -P exp_update_perf_10" &

wait

echo done

