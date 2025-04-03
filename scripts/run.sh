#!/usr/bin/env bash

# --- Configuration ---
base_input_path="../input/"
base_output_path="../output/hadoop/"
# Use a bash array for the list of raw input classes
raw_input_classes=("HadoopWordCount" "HadoopWordPairs")
# Path to the JAR relative to where this script is run
hadoop_jar_path="../output/HadoopWordCount.jar"
# --- End Configuration ---

# --- Helper Functions ---
show_usage() {
  # Get script name
  local script_name
  script_name=$(basename "$0")
  echo "Usage:" >&2 # Send usage info to standard error
  echo "  For raw input classes (${raw_input_classes[*]}):" >&2
  echo "    $script_name <class> <prefix1> [<prefix2> ...]" >&2
  echo "    Example: $script_name HadoopWordCount AA AB AC" >&2
  echo "" >&2
  echo "  For processed input:" >&2
  echo "    $script_name <class> <input_job>/<input_range>" >&2
  echo "    Example: $script_name WordCountFilter HadoopWordCount/AA-AB" >&2
  exit 1 # Exit with an error code
}

is_raw_input_class() {
  local class_to_check="$1"
  for raw_class in "${raw_input_classes[@]}"; do
    if [[ "$class_to_check" == "$raw_class" ]]; then
      return 0 # Success (is a raw input class)
    fi
  done
  return 1 # Failure (not a raw input class)
}
# --- End Helper Functions ---

# --- Argument Validation ---
if [[ -z "$1" ]]; then
  echo "Error: Missing class name argument" >&2
  show_usage
fi
if [[ -z "$2" ]]; then
  echo "Error: Missing input specification argument(s)" >&2
  show_usage
fi

hadoop_class="$1"
shift # Remove class name from arguments, $@ now contains the rest

# --- Determine Input Mode ---
if is_raw_input_class "$hadoop_class"; then
  # --- RAW INPUT PROCESSING ---
  echo "[INFO] Processing RAW INPUT mode for class '$hadoop_class'"

  if [[ $# -eq 0 ]]; then
      echo "Error: Raw input mode requires at least one prefix argument." >&2
      show_usage
  fi

  first_prefix="$1"
  # Store all prefixes provided
  input_prefixes=("$@")
  # Get the last prefix using array indexing (${array[-1]})
  last_prefix="${input_prefixes[-1]}"

  # Build the comma-separated input paths
  input_paths_array=()
  for prefix in "${input_prefixes[@]}"; do
    input_paths_array+=("${base_input_path}${prefix}")
  done

  # Join the array elements with a comma
  input_paths=$(IFS=,; echo "${input_paths_array[*]}")

  # Construct output path
  output_path="${base_output_path}${hadoop_class}/${first_prefix}-${last_prefix}"

else
  # --- PROCESSED INPUT PROCESSING ---
  echo "[INFO] Processing PROCESSED INPUT mode for class '$hadoop_class'"

  if [[ $# -ne 1 ]]; then
      echo "Error: Processed input mode requires exactly one argument: <JobName>/<Range>" >&2
      show_usage
  fi

  input_spec="$1" # The first remaining argument is the spec

  # Validate format JobName/XX-YY
  if ! echo "$input_spec" | grep -q '/'; then
      echo "Error: Input spec '$input_spec' must use JobName/XX-YY format." >&2
      show_usage
  fi

  # Parse the input spec using parameter expansion
  input_job="${input_spec%/*}"  # Everything before the last /
  input_range="${input_spec#*/}" # Everything after the first /

  # Check if parsing resulted in empty parts (e.g., "job/" or "/range")
  if [[ -z "$input_job" || -z "$input_range" ]]; then
      echo "Error: Invalid format '$input_spec' - requires non-empty JobName and Range." >&2
      show_usage
  fi

  # Construct paths for processed input
  input_paths="${base_output_path}${input_spec}" # The whole spec forms the input path
  output_path="${base_output_path}${hadoop_class}/${input_range}" # Only range used for output subdir

fi

# --- Execute Hadoop Command ---
echo "[INFO] HDFS Input Paths:  $input_paths"
echo "[INFO] HDFS Output Path: $output_path"

# Set Hadoop client options (export makes it available to the hadoop command)
export HADOOP_CLIENT_OPTS="-Xmx16g ${HADOOP_CLIENT_OPTS}" # Append safely
echo "[INFO] Using HADOOP_CLIENT_OPTS: $HADOOP_CLIENT_OPTS"

# Build command as an array for safety with spaces/special chars
hadoop_cmd=(
  hadoop jar
  "$hadoop_jar_path"
  "$hadoop_class"
  "$input_paths"
  "$output_path"
)

echo "[INFO] Executing: ${hadoop_cmd[*]}" # Display the command clearly

# Execute the command
"${hadoop_cmd[@]}"
exit_status=$? # Capture the exit status immediately

if [[ $exit_status -ne 0 ]]; then
  echo "[ERROR] Hadoop command failed with exit code $exit_status" >&2
  exit $exit_status
fi

echo "[INFO] Hadoop command completed successfully."
exit 0