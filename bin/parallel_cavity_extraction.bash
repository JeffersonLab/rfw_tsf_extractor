#!/bin/bash

# This figures out the real directory where this script lives
DIR="$( cd "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" >/dev/null 2>&1 && pwd )"
app_dir=$(readlink -f $DIR/..)

now=$(date +%F_%T)
nodes=$app_dir/nodefile
labels_dir=$app_dir/labeled-examples/processed
log_dir=$app_dir/log/cavity_$now
job_log=$log_dir/cavity_${now}_jobs.log

cmd="$app_dir/bin/do_cavity_extraction.bash {} $log_dir"

mkdir -p $log_dir

ls $labels_dir/* | parallel -j+0 --progress --sshloginfile $nodes --sshdelay 0.1 --workdir $app_dir --joblog $job_log $cmd
