#!/bin/bash

# This figures out the real directory where this script lives
DIR="$( cd "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" >/dev/null 2>&1 && pwd )"
app_dir=$(readlink -f $DIR/..)

log_dir=$2
log_file=$log_dir/trip.$(basename $1).log

source $app_dir/venv/bin/activate
python $app_dir/python/extract.py $1 trip > $log_file 2>&1
