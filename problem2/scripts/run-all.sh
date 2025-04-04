#!/bin/bash

# Function to run dependent jobs (Move it to the top)
function run_dependent_job {
    local dep_class="$1"
    local parent_class="$2"
    shift 2
    local input_dirs=("$@")

    # Convert array to first-last format
    local first_dir="${input_dirs[0]}"
    local last_dir="${input_dirs[-1]}"
    local input_spec="${parent_class}/${first_dir}-${last_dir}"

    echo
    echo "======================================"
    echo "Running $dep_class with input: $input_spec"
    echo "======================================"

    # Set stats file name for this job class
    local stats_file="stats/${dep_class}_stats.csv"
    local job_type="Dependent"

    # Create stats file header if it doesn't exist
    if [ ! -f "$stats_file" ]; then
        echo "InputSpec,StartTime,EndTime,ElapsedTime" > "$stats_file"
    fi

    # Get start time with nanoseconds precision
    local start_time=$(date +"%Y-%m-%d %H:%M:%S.%N")
    local start_sec=$(date +%s.%N)

    # Run the dependent job
    ./run.sh "$dep_class" "$input_spec"
    local exit_code=$?

    # Get end time with nanoseconds precision
    local end_time=$(date +"%Y-%m-%d %H:%M:%S.%N")
    local end_sec=$(date +%s.%N)

    # Calculate elapsed time in seconds with 3 decimal places
    local elapsed_time=$(echo "$end_sec - $start_sec" | bc -l | xargs printf "%.3f")

    # Write to individual job stats file
    echo "$input_spec,$start_time,$end_time,$elapsed_time" >> "$stats_file"

    # Append to temp stats file
    echo "$dep_class,$job_type,$input_spec,$start_time,$end_time,$elapsed_time" >> "$ALL_STATS_TEMP"

    # Check for errors
    if [ $exit_code -ne 0 ]; then
        echo "ERROR: Job $dep_class with input $input_spec failed with exit code $exit_code"
        return $exit_code
    fi

    return 0
}

# Configuration
SUBDIRS=("AA" "AB" "AC" "AD" "AE" "AF" "AG" "AH" "AI" "AJ" "AK")
RAW_CLASSES=("SparkWordCount" "SparkWordPairs")
WORDCOUNT_DEPS=("WordCountFilter" "WordCountTop")
WORDPAIRS_DEPS=("WordPairsFilter" "WordNumberPairsTop")

# Create stats directory if it doesn't exist
mkdir -p stats

# Initialize master stats file
ALL_STATS_FILE="stats/all-stats.csv"
ALL_STATS_TEMP="stats/all-stats.tmp"

# Write header to temp file
echo "JobName,JobType,InputSpec,StartTime,EndTime,ElapsedTime" > "$ALL_STATS_TEMP"

# Initialize variables
input_dirs=()

# Process each subdirectory incrementally
for dir in "${SUBDIRS[@]}"; do
    # Add the current directory to the input list
    input_dirs+=("$dir")
    input_spec="${input_dirs[@]}"
    input_spec="${input_spec// /,}"

    # Run raw input jobs first
    for class in "${RAW_CLASSES[@]}"; do
        echo
        echo "======================================"
        echo "Running $class with input: ${input_dirs[@]}"
        echo "======================================"

        # Set stats file name for this job class
        stats_file="stats/${class}_stats.csv"
        job_type="RawInput"

        # Create stats file header if it doesn't exist
        if [ ! -f "$stats_file" ]; then
            echo "InputDirs,StartTime,EndTime,ElapsedTime" > "$stats_file"
        fi

        # Get start time with nanoseconds precision
        start_time=$(date +"%Y-%m-%d %H:%M:%S.%N")
        start_sec=$(date +%s.%N)

        # Run the job
        ./run.sh "$class" "${input_dirs[@]}"
        exit_code=$?

        # Get end time with nanoseconds precision
        end_time=$(date +"%Y-%m-%d %H:%M:%S.%N")
        end_sec=$(date +%s.%N)

        # Calculate elapsed time in seconds with 3 decimal places
        elapsed_time=$(echo "$end_sec - $start_sec" | bc -l | xargs printf "%.3f")

        # Write to individual job stats file
        echo "$input_spec,$start_time,$end_time,$elapsed_time" >> "$stats_file"

        # Append to temp stats file
        echo "$class,$job_type,$input_spec,$start_time,$end_time,$elapsed_time" >> "$ALL_STATS_TEMP"

        # Check for errors
        if [ $exit_code -ne 0 ]; then
            echo "ERROR: Job $class with input ${input_dirs[@]} failed with exit code $exit_code"
            exit $exit_code
        fi

        # Now run dependent jobs if this was a parent job
        if [ "$class" = "SparkWordCount" ]; then
            # Run WordCount dependent jobs
            for dep in "${WORDCOUNT_DEPS[@]}"; do
                run_dependent_job "$dep" "$class" "${input_dirs[@]}"
                if [ $? -ne 0 ]; then exit $?; fi
            done
        elif [ "$class" = "SparkWordPairs" ]; then
            # Run WordPairs dependent jobs
            for dep in "${WORDPAIRS_DEPS[@]}"; do
                run_dependent_job "$dep" "$class" "${input_dirs[@]}"
                if [ $? -ne 0 ]; then exit $?; fi
            done
        fi
    done
done

# Sort the all-stats file by JobName
echo "Sorting all-stats.csv by JobName..."
(
    # Write header first
    head -n 1 "$ALL_STATS_TEMP"
    # Then write sorted data (skip header line)
    tail -n +2 "$ALL_STATS_TEMP" | sort -t, -k1,1
) > "$ALL_STATS_FILE"

rm "$ALL_STATS_TEMP"

echo
echo "All jobs completed successfully. Statistics saved to:"
echo "$ALL_STATS_FILE (sorted by JobName)"
for class in "${RAW_CLASSES[@]}" "${WORDCOUNT_DEPS[@]}" "${WORDPAIRS_DEPS[@]}"; do
    echo "stats/${class}_stats.csv"
done

exit 0