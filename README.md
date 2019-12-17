# User Guide / README

## Overview

This "app" is designed to provide a template for preprocessing RF waveform label
data (faulting cavity and fault type labels), and for performing feature extraction
using tsfresh.  A script manages processing raw label files (provided by T. Powers),
into a directory of label files, one per RF fault event.  Additional scripts manage
the work of feature extraction based on the processed label files present in the
labeled-examples/processed directory.

Feature extraction is performed in parallel (thanks to GNU parallel) across the nodes
specified in the nodelist file.  The feature extraction work is broken into two main
pieces, the extraction needed for the cavity model and the extraction needed for the
fault type (a.k.a., "trip") model, as represented by the two parallel\_*.bash scripts
in bin/.  Each parallel\_*.bash script ultimately calls python/extract.py on each 
file in labeled-examples/processed.  These jobs are meant to be run by parallel, so
tsfresh.extract_features is called with n\_jobs=1 (i.e., no internal parallelization).

To use this template, simply make a copy of the app somewhere with sufficient
storage and accessible by the hosts that will be running feature extraction code.
See the setup section for additional details.

After setting up the rfw_tsf_extractor app as described in the Setup section, simply
follow the steps in the Workflow section to perform the data processing and 
feature extraction.

## Directory/File Overview

| Folder/File | Description |
| ----------- | ----------- |
| bin/ | for executable files |
| extracted/ | for files containing extracted features |
| labeled-examples/ | for files containing labeled examples |
| nodefile | a file that controls which remote nodes parallel will try to run jobs |
| python/ | contains python code for performing feature extraction |
| waveform-data/ | contains the harvested waveform data on which extraction is performed |
| venv/ | Directory that is created to contain Python virtual environment. |
| requirements.txt | pip requirements for creating python virtual environment |

## Setup

If you are only running this on a single node (i.e., no parallelization) then
the only step is to place a copy of rfw_tsf_extractor into a location directly
accessible by that node (local storage, NFS, etc.).  Then follow the Workflow
section below.

If you want to run this on multiple nodes, you will need to place the copy
of rfw_tsf_extractor in an area accessible to all nodes.  The most straight-
forward way to do this is to place the app on existing shared storage available
to all of the nodes.  For the Spring-2018 run, I setup several compute servers,
installed this app on one of them, and exported it via NFS to the others.  NOTE:
if you follow this pattern, the app must be appear in the same location on
all servers since parallel calls include with full paths to scripts and data.

Update the nodefile to contain the hostnames of the hosts that will be running
the feature extraction workflow, one per line.  GNU parallel uses a special ':'
character to represent localhost (':' means no ssh).  These nodes must be
accessible via SSH for the parallel jobs to be run on them.

## Workflow

0) Copy in labeled examples and waveform data
    1) Place any files containing labeled examples in labeled-examples/raw.
       These files should be TSVs of Tom's usual format, e.g.,
       zone cavity  cav#    fault   time

    2) Copy over (or link) waveform data to waveform-data.  The scripts expect
       the usual rf/<zone>/<date>/<time>/<capture_file> structure under
       waveform-data.

1) Setup the Python Virtual Environment

    All python code in this app was developed against stock Python 3.6.9 using the
    packages listed in requirements.txt.  Please install a similar python interpreter
    and run the following commands in a bash shell.

    cd /path/to/rfw_tsf_extractor
    /path/to/python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    deactivate

    Now you have a suitable python environment.  Any scripts that deal with python will
    be loading this environment before launching code.

2) Process files containing labeled examples (Tom's results)

    1) Run the following script to turn the "raw" label files under labeled-examples/raw
       into a directory of files under labeled_examples/processed, which contain a single
       event/label and are named for the event (<zone>-<timestamp>.tsv).
       
       bin/process_raw_label_files.bash

       Note that this script performs several ancillary functions besides actually
       processing the raw files.
       - It deletes all files under processed/ prior to processing.
       - It produces labeled-examples/event_reduction.log that describes the action
         it took for each event.
       - It writes report information to stdout labeled-examples/process.log
       - It writes a single file, master.csv, containing all data from the individual label
         files.

3) Update nodefile to reflect where to run feature extraction jobs.  The current setup
   a job per core on all listed systems, which completely swamps the systems in nodefile.
   The job management is done using GNU's parallel.

4) Run parallel extraction scripts to extract features.  On our 68 hyper thread setup,
   each script takes on the order of 12 hours (i.e., overnight-ish) to process 407 events.
   These script will max out CPUs on all of the hosts listed in nodefile
   
    1) For cavity model feature extraction run
    
      > bin/parallel_cavity_extraction.bash
    
    2) For trip model feature extraction run:
    
      > bin/parallel_trip_extraction.bash

5) Review the results
    1) All logs are written to log/\<cavity\|trip\>\_\<timestamp\> including individual tsfresh
       job output, and the GNU parallel jobs_log (\<cavity\|trip\>\_\<timestamp\>\_jobs.log)
    2) The results of each tsfresh job is written to the extracted/ directory.
       Each event will have to files per parallel_*_extraction.bash script, a *_X.csv
       for the extracted features and a *_y.csv for the matching label info.  Should
       the tsfresh job encounter an error, then no CSV file will be written.  Examine
       the GNU parallel jobs log for exit status of each job to verify that all of the
       output (or lack of output) is understood.
