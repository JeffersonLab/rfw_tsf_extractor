#!/bin/env bash

# This figures out the real directory where this script lives
DIR="$( cd "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" >/dev/null 2>&1 && pwd )"
app_dir=$(readlink -f $DIR/..)

python_dir=$app_dir/python
labeled_dir=$app_dir/labeled-examples
processed_dir=$labeled_dir/processed

source $app_dir/venv/bin/activate

if [ -d "$processed_dir" ] ; then
    rm -f $processed_dir/*
fi
python $python_dir/process.py $labeled_dir |& tee $labeled_dir/process.log
