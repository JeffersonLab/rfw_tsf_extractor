import pandas as pd
import numpy as np
import os
import sys
import glob

from tsfresh import extract_features, extract_relevant_features, select_features
from tsfresh.feature_extraction import ComprehensiveFCParameters, EfficientFCParameters, MinimalFCParameters
from tsfresh.utilities.dataframe_functions import impute

# Where this script currently lives
python_dir = os.path.dirname(os.path.realpath(__file__))

# The root of the rfw_tsf_extractor app
app_dir = os.path.realpath(os.path.join(python_dir, '..'))

# Locations within the app
data_dir = os.path.join(app_dir, 'waveform-data', 'rf')
label_dir = os.path.join(app_dir, 'labeled-examples', 'processed')
out_dir = os.path.join(app_dir, 'extracted')

# Essentially how many tsfesh processes to run in parallel.  1 is good if wrapping this in a 'parallel' call
tsf_jobs = 1

if __name__ == "__main__":
    # Process simple command line arguments
    if len(sys.argv) != 3:
        print("Usage: {} <label_file> <(cavity|trip)>".format(sys.argv[0]))
        exit(1)

    # Label file can be relative to the processed lablel-directory or absolute.
    # Join discards earlier elements if a later element starts with a root '/'
    label_file = os.path.join(label_dir, sys.argv[1])
    extract_type = sys.argv[2]
    
    # Validate label file
    if not os.path.exists(label_file):
        print("Error: label file not found: {}".format(label_file))
        exit(1)

    # Validate extraction type
    valid_types = ('cavity', 'trip')
    if extract_type not in valid_types:
        print("Error: invalid extraction type specified '{}'.  Valid options are {}".format(extract_type, valid_types))
        exit(1)

    # Read in the labeled data file
    labeled_df = pd.read_table(label_file, sep='\t')
    
    # These files could contain many labeled events, but this process expects there to
    # only one labeled event per file (with 5 columns)
    if labeled_df.shape != (1,5):
        print("Error: '{}' has more than one row".format(label_file))
        exit(1)

    # Needed for mapping zones in the label files to capture file names on disk
    zone_dict = {'0L04': 'R04', '1L22': 'R1M', '1L23': 'R1N', '1L24': 'R1O', '1L25': 'R1P',
                '1L26': 'R1Q', '2L22': 'R2M', '2L23': 'R2N', '2L24': 'R2O', '2L25': 'R2P', '2L26': 'R2Q'}

    # Save the zone where the event occurred
    zone = str(labeled_df.zone[0])

    # Date and time are a sinlge space separated field
    date, time = str(labeled_df.time[0]).split(" ")

    # Change date format to match that used on file system
    date = date.replace("/", "_")

    # Change time format to match that used on file system.  The files also include a
    # fractional second component, so add a shell glob wildcard.
    time = time.replace(":", "")
    time_glob = time + ".?"

    # Save a timestamp that can be used as part of an event identifier
    timestamp = str(labeled_df.time[0]).replace(":", "").replace(" ", "_").replace("/", "-")

    # File path string that should match the event directory associated with this label
    event_glob = os.path.join(data_dir, zone, date, time_glob)

    # Actual path to event directory (it's a list since it could have multiple matches)
    event_dir_list = glob.glob(event_glob)

    # Check that we got the directory we expected.
    if len(event_dir_list) == 0:
        print("Error: no event dirs found that match event glob '{}'".format(event_glob))
        exit(1)
    if len(event_dir_list) > 1:
        print("Error: more than one event dir matched label - '{}'".format(event_list))
        exit(1)

    # Grab the event directory
    event_dir = event_dir_list[0]

    # Process the data to extract the features needed for cavity labeling models
    if extract_type == 'cavity':

        # Get a list of the capture files associated this event
        capture_files = os.listdir(event_dir)

        # Check that we have all eight of the files
        if len(capture_files) != 8:
            print("Error: event {} {} has {} files, not 8".format(zone,
                timestamp, len(capture_files)))
            exit(1)

        # Check that we do not have any duplicates
        cav_list = []
        cav_set = set()
        for file in capture_files:
            cav = file[0:4]
            cav_list.append(cav)
            cav_set.add(cav)
            if len(cav_list) != len(cav_set):
                print("Error: Duplicate capture files found for a cavity '{}'".format(cav))
                exit(1)
        
        waveforms_df = pd.DataFrame()

        event_id = 1 # Since the file only has one label, just assign a static ID.  Needed by tsfresh.
        for m in range(0,8):
            f = os.path.join(event_dir, capture_files[m])
            df = pd.read_table(f, sep='\t')
            sLength = len(df['Time'])
            tStep = (df.Time[2] - df.Time[1]) # in milliseconds
            df['id'] = pd.Series(event_id, index=df.index)
            col = ['Time', 
                    f'{m+1}_IMES', f'{m+1}_QMES', f'{m+1}_GMES', f'{m+1}_PMES', f'{m+1}_IASK', f'{m+1}_QASK', 
                    f'{m+1}_GASK', f'{m+1}_PASK', f'{m+1}_CRFP', f'{m+1}_CRFPP', f'{m+1}_CRRP', f'{m+1}_CRRPP', 
                    f'{m+1}_GLDE', f'{m+1}_PLDE', f'{m+1}_DETA2_', f'{m+1}_CFQE2_', f'{m+1}_DFQES',
                    'id']
            df.columns=col
            waveforms_df = pd.concat([waveforms_df, df], axis=1, sort=False)
            #print("j = {0:}, k = {1:}, input shape = {2:}, master shape = {3:}, time step = {4:3.2f} ms".format(j, k, df.shape, master_df.shape, tStep))
            waveforms_df = waveforms_df.loc[:,~waveforms_df.columns.duplicated()]
    

        # Remove any trailing '_' from column names
        mapper = {
                  "1_DETA2_":"1_DETA2", "2_DETA2_":"2_DETA2", "3_DETA2_":"3_DETA2",
                  "4_DETA2_":"4_DETA2", "5_DETA2_":"5_DETA2", "6_DETA2_":"6_DETA2",
                  "7_DETA2_":"7_DETA2", "8_DETA2_":"8_DETA2"
                 }
        waveforms_df = waveforms_df.rename(columns=mapper)

        # Since this cavity problem has so many waveforms (8*17) and takes a long time for tsfresh to analyze all of them,
        # we down select to only extract features for the waveforms that Tom is looking at when making his labels
        select_columns = ["Time", "id", "1_GMES", "1_GASK", "1_CRFP", "1_DETA2", "2_GMES", "2_GASK", "2_CRFP", "2_DETA2", "3_GMES", 
               "3_GASK", "3_CRFP", "3_DETA2", "4_GMES", "4_GASK", "4_CRFP", "4_DETA2", "5_GMES", "5_GASK", "5_CRFP", 
               "5_DETA2", "6_GMES", "6_GASK", "6_CRFP", "6_DETA2", "7_GMES", "7_GASK", "7_CRFP", "7_DETA2", "8_GMES", 
               "8_GASK", "8_CRFP", "8_DETA2"]
        waveforms_df = waveforms_df[select_columns]

        # Grab the label and include identifying info
        y = pd.DataFrame({'zone': zone, 'time': timestamp, 'label': labeled_df.cavity})

        # Perform the cavity extraction
        extraction_settings = ComprehensiveFCParameters()
        X = extract_features(waveforms_df.astype('float64'),
                             column_id="id",
                             column_sort="Time",
                             impute_function=impute,
                             default_fc_parameters=extraction_settings,
                            disable_progressbar=True,
                             n_jobs=tsf_jobs
                            )

        # Add the ID info to the tsfresh output so we can join files together in the future
        X['zone'] = zone
        X['time'] = timestamp


        # Save the results and the label in files for later access
        X.to_csv(os.path.join(out_dir, 'cavity_{}_{}_X.csv'.format(zone, timestamp)), index=False)
        y.to_csv(os.path.join(out_dir, 'cavity_{}_{}_y.csv'.format(zone, timestamp)), index=False)

        print("Completed cavity feature extraction")

    # Extract the features needed to train our trip classifier model
    if extract_type == 'trip':
        # Here we use Tom's labeled cavity to filter this process.  If he said it was a 'Multi Cav Turn off' with
        # cavity == 0, then we just skip it, since our cavity models will produce this result
        cavity_label = str(labeled_df.cavity[0])
        if cavity_label == 0:
            print("No feature extraction needed since cavity label was '{}'".format(cavity_label))
            exit()

        # Get a list of the capture files associated this event
        capture_files = os.listdir(event_dir)

        # Check that we have one capture file for the identified cavity
        epics_cav = zone_dict[zone] + cavity_label
        cav_file_count = 0
        for file in capture_files:
            if file[0:4] == epics_cav:
                cav_file_count += 1
        if cav_file_count == 0:
            print("Error: Missing capture file for cavity label {} / EPICS cavity name {}".format(cavity_label, epics_cav))
            exit(1)
        elif cav_file_count != 1:
            print("Error: Found {} capture files for cavity label {} / EPICS cavity name {}".format(cav_file_count, cavity_label, epics_cav))
            exit(1)
        
        # Go through the list of files and grab the file that matches the needed cavity
        cavity_file = ""
        for file in capture_files:
            if file[0:4] == epics_cav:
                cavity_file = os.path.join(event_dir, file)
                break

        # Read in the file.  Since only one label/event, assign single ID number for use by tsfresh
        waveforms_df = pd.read_table(cavity_file, sep='\t')
        waveforms_df['id'] = pd.Series(1, index=waveforms_df.index)

        # Grab the label and include identifying info
        y = pd.DataFrame({'zone': zone, 'time': timestamp, 'label': labeled_df.fault})

        # Calculate all of time series features available
        extraction_settings = ComprehensiveFCParameters()
        X = extract_features(waveforms_df.astype('float64'),
                            column_id="id",
                            column_sort="Time",
                            impute_function=impute,
                            default_fc_parameters=extraction_settings,
#                            disable_progressbar=True,
                            n_jobs=tsf_jobs
                            )

        # Add the ID info to the tsfresh output so we can join files together in the future
        X['zone'] = zone
        X['time'] = timestamp

        # Save the results and the label in files for later access
        X.to_csv(os.path.join(out_dir, 'trip_{}_{}_X.csv'.format(zone, timestamp)), index=False)
        y.to_csv(os.path.join(out_dir, 'trip_{}_{}_y.csv'.format(zone, timestamp)), index=False)

        print("Completed trip feature extraction") 
