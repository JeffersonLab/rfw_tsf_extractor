#!/bin/bash

# This figures out the real directory where this script lives
DIR="$( cd "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" >/dev/null 2>&1 && pwd )"
app_dir=$(readlink -f $DIR/..)

now=$(date +%F_%T)
nodes=$app_dir/nodefile
labels_dir=$app_dir/labeled-examples/processed
log_dir=$app_dir/log/trip_$now
job_log=$log_dir/trip_${now}_jobs.log

cmd="$app_dir/bin/do_trip_extraction.bash {} $log_dir"

mkdir -p $log_dir

ls $labels_dir/* | parallel --progress --sshloginfile $nodes --sshdelay 0.1 --workdir $app_dir --joblog $job_log $cmd
