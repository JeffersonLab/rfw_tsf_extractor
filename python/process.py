import os
import sys

# This script reads in a directory of data files and writes out an directory of data files
# where each file contains the header from it's raw file, and exactly one line of the raw file.
# We assume each raw file is a TSV, contains the same header, and has the same structure.

debug = False

# Count how many unique events are present
def count_events(events):
    return len(events)

# Count how may labels are present
def count_labels(events):
    n_labels = 0
    for event in events:
        n_labels += len(events[event])

    return n_labels
        
# Count how many events have multiple labels
def count_events_with_multiple_labels(events):
    n_events = 0
    for event in events:
        if len(events[event]) > 1:
            n_events += 1

    return n_events

# Count how many labels are associated with events that have multiple labels
def count_duplicate_labels(events):
    n_dups = 0
    for event in events:
        lines = events[event]
        if len(lines) > 1:
            n_dups += len(lines) 
    return n_dups
    
# Count how many events have multiple labels that do not match each other
def count_events_with_mismatched_multiple_labels(events):
    n_events = 0
    for event in events:
        lines = events[event]
        if len(lines) > 1:
            for i in range(len(lines)):
                if lines[0] != lines[i]:
                    n_events += 1
                    break
    return n_events

# Count how many mismatched labels are associated with events
def count_mismatched_labels(events):
    n_labels = 0
    for event in events:
        lines = events[event]
        all_match = True
        if len(lines) > 1:
            for i in range(len(lines)):
                if lines[0] != lines[i]:
                    all_match = False
                    break
        if not all_match:
            n_labels += len(lines)

    return n_labels
        
# Print out events and labels for events with mismatched label
def print_mismatched_labels(events):
    for event in events:
        lines = events[event]
        if len(lines) > 1:
            all_match = True
            for i in range(len(lines)):
                if lines[0] != lines[1]:
                    all_match = False
            if not all_match:
                print("Found mismatch for {}".format(event))
                for i in lines:
                    print("    " + i)


# This returns a similar dictionary to the supplied events, but without events that have
# mismatched labels, and duplicate labels are removed.  Values of the returned dictionary are
# now strings (the label line), and not arrays since duplicates have been removed.
def remove_duplicates_and_mismatches(events, log_file):
    events_reduced = dict()

    with open(log_file, "w") as log:
        fmt = "{}\t{}\t{}\t{}\n"
        log.write(fmt.format("event", "action", "label_lines_found", "label_lines_removed"))

        for event in events:
            lines = events[event]
            if len(lines) < 1:
                log.write(fmt.format(event, "skipped_no_labels_found", str(len(lines)), str(len(lines))))
            elif len(lines) == 1:
                events_reduced[event] = lines[0]
                log.write(fmt.format(event, "included", str(len(lines)), "0"))
            else:
                first_line = lines[0]
                has_mismatch = False
                for i in range(1, len(lines)):
                    if first_line != lines[i]:
                        has_mismatch = True
                        break
                if has_mismatch:
                    # Don't include an event with a mismatch
                    log.write(fmt.format(event, "skipped_mismatched_labels", str(len(lines)), str(len(lines))))
                else:
                    # Just include the first line for events with duplicates
                    events_reduced[event] = first_line
                    log.write(fmt.format(event, "removed_duplicates", str(len(lines)), str(len(lines)-1)))

    return events_reduced


# For each event/label pair print out a TSV label file.  Expects events to be keyed on event identifier with a single label line as a value.  Returns the number of files written
def print_label_files(events, out_dir):
    files_written = 0
    for event in events:
        line = events[event]
        fields = line.split('\t')
        zone = fields[0]
        ts = fields[4].replace("/", "-").replace(" ", "_").replace(":","")
        filename = "{}_{}.csv".format(zone, ts)

        with open(os.path.join(out_dir, filename), 'w') as out:
            files_written += 1
            out.write(header)
            out.write(line + "\n")
            if debug:
                print("Wrote file: {} {} - {}".format(zone, ts, line))

    return files_written

# Similar to print_label_files, except that this prints out a single file with of all the label lines
def print_master_label_file(events, out_dir):
    header = "zone	cavity	cav#	fault	time\n"
    with open(os.path.join(out_dir, "master.csv"), "w") as out:
        out.write(header)
        for event in events:
            line = events[event]
            out.write(line + "\n")


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Error: missing labeled_dir argument")
        print("Usage: process.py </path/to/labeled-examples>")
        exit(1)

    # Where the label processing happens.  Should contain raw and processed directories.
    labeled_dir = sys.argv[1]

    if not os.path.isdir(labeled_dir):
        print("Error: directory not found '{}'".format(labeled_dir))
        exit(1)

    # Where the data files that are to be process live
    raw_dir = os.path.join(labeled_dir, 'raw')

    # Where to put the processed files
    process_dir = os.path.join(labeled_dir, 'processed')

    # This is the header we expect in all files
    exp_header = "zone	cavity	cav#	fault	time\n"

    # Setup a datastructure to detect duplicate events.  keyed on <zone>_<ts>
    events = {}

    # Iterate through the files in the raw directory and build a dictionary keyed on events
    # with an array of labels found for each event.  We'll print out summary information,
    # and then print label files for each "good" event
    for f in os.listdir(raw_dir):
        if not os.path.isfile:
            continue

        if debug:
            print("\n\n\n{}".format(f))

        # Read each file line by line and create a new TSV file for each
        # labeled example we encounter
        with open(os.path.join(raw_dir, f), 'r') as fh:
            # Toss the header line by reading another one in loop
            line = fh.readline()
            header = line
            if debug:
                print("Skipping header: {}".format(header))
            if header != exp_header:
                print("Error: Unexpected header: '{}'".format(header))
            while line:
                line = fh.readline().rstrip()
                if not line:
                    if debug:
                        print("Skipping: {}".format(line))
                    break
                if line.startswith("#"):
                    if debug:
                        print("Skipping: {}".format(line))
                    continue
                fields = line.split('\t')
                zone = fields[0]
                ts = fields[4].replace("/", "-").replace(" ", "_").replace(":","")
                filename = "{}_{}.csv".format(zone, ts)

                # Check if we've seen this event.  If not, create an array at that key.
                # Otherwise, add to the array.
                event_key = "{}_{}".format(zone,ts)
                if event_key in events:
                    events[event_key].append(line)
                else:
                    events[event_key] = [line]
                    
                if debug:
                    print("Working: {} {} - {}".format(zone, ts, line))

                if os.path.exists(os.path.join(process_dir, filename)):
                    if debug:
                        print("Warning: Found duplicate file - {} {}".format(zone, ts))
                    continue
                

    # Check to see if we have any duplicates and print them out
    num_total_events = count_events(events)
    num_total_labels = count_labels(events)
    num_events_with_multiple_labels = count_events_with_multiple_labels(events)
    num_duplicate_labels = count_duplicate_labels(events)
    num_events_with_mismatched_labels = count_events_with_mismatched_multiple_labels(events)
    num_mismatched_labels = count_mismatched_labels(events)

    print("\n\nChecking for duplicates with mismatched labels")
    if num_events_with_mismatched_labels > 0:
        print_mismatched_labels(events)

    print("\n\n#### Summary ####\n")
    print("Note: event == unique zone/timestamp, label == row in label_file\n")
    print("Number of events: " + str(num_total_events))
    print("Number of labels: " + str(num_total_labels))
    print("Number of events with multiple labels: " + str(num_events_with_multiple_labels))
    print("Number of duplicate labels: " + str(num_duplicate_labels))
    print("Number of 'extra' labels: " + str(num_duplicate_labels - num_events_with_multiple_labels))
    print("")
    print("Number of events with mismatched labels: " + str(num_events_with_mismatched_labels))
    print("Number of mismatched labels: " + str(num_mismatched_labels))

    events_reduced = remove_duplicates_and_mismatches(events, os.path.join(labeled_dir, "event_reduction.log"))

    num_files_written = print_label_files(events_reduced, process_dir)
    print("")
    print("Number of label files written: " + str(num_files_written))

    print_master_label_file(events_reduced, labeled_dir)
