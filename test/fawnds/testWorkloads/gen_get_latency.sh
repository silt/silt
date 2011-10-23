#!/bin/bash

source common.sh

## for GET latency and GET latency breakdown
generate_load exp_100GB 1000 "-P exp_100GB" &
generate_trans exp_100GB_lookup_perf 1000 "-P exp_100GB -P exp_lookup_perf" &

wait

echo done

