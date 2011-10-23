#!/bin/bash

source common.sh

## for tiny data
generate_load exp_tiny_6.4GB 40 "-P exp_tiny_6.4GB" &
generate_trans exp_tiny_6.4GB_update_perf_50 40 "-P exp_tiny_6.4GB -P exp_update_perf_50" &

wait

echo done

