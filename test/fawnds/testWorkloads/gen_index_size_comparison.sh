#!/bin/bash

source common.sh

## for index size change graph
generate_load exp_50GB 1000 "-P exp_50GB" &
generate_trans exp_50GB_insert_perf_10 1000 "-P exp_50GB -P exp_insert_perf_10_500M" &

wait

echo done

