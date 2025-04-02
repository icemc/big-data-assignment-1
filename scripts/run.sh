#!/bin/bash

# Configuration
BASE_INPUT_PATH="/user/Ludovic/articles/articles/"
BASE_OUTPUT_PATH="/user/Ludovic/output/"
RAW_INPUT_CLASSES=("HadoopWordCount" "HadoopWordPairs")

# Show usage information
show_usage() {
  echo "Usage:"
  echo "  For raw input classes (${RAW_INPUT_CLASSES[*]}):"
  echo "    $0 <class> <prefix1> [<prefix2> ...]"
  echo "    Example: $0 HadoopWordCount AA AB AC"
  echo
  echo "  For processed input:"
  echo "    $0 <class> <input_job>/<input_range>"
  echo "    Example: $0 WordCountFilter HadoopWordCount/AA-AB"
  exit 1
}

# Validate arguments
if [ $# -lt 2 ]; then
  echo "Error: Missing arguments"
  show_usage
fi

# Get arguments
HADOOP_CLASS="$1"
shift

# Check if raw input class
IS_RAW=0
for class in "${RAW_INPUT_CLASSES[@]}"; do
  if [ "$HADOOP_CLASS" = "$class" ]; then
    IS_RAW=1
    break
  fi
done

if [ $IS_RAW -eq 1 ]; then
  # RAW INPUT PROCESSING
  FIRST_PREFIX="$1"
  LAST_PREFIX="$1"
  INPUT_PATHS="${BASE_INPUT_PATH}$1"

  shift
  while [ $# -gt 0 ]; do
    LAST_PREFIX="$1"
    INPUT_PATHS="${INPUT_PATHS},${BASE_INPUT_PATH}$1"
    shift
  done

  OUTPUT_PATH="${BASE_OUTPUT_PATH}${HADOOP_CLASS}/${FIRST_PREFIX}-${LAST_PREFIX}"
else
  # PROCESSED INPUT PROCESSING
  INPUT_SPEC="$1"
  if [[ "$INPUT_SPEC" != *"/"* ]]; then
    echo "Error: Must use JobName/XX-YY format"
    show_usage
  fi

  IFS='/' read -r INPUT_JOB INPUT_RANGE <<< "$INPUT_SPEC"
  if [ -z "$INPUT_RANGE" ]; then
    echo "Error: Invalid format - missing range after /"
    show_usage
  fi

  INPUT_PATHS="${BASE_OUTPUT_PATH}${INPUT_SPEC}"
  OUTPUT_PATH="${BASE_OUTPUT_PATH}${HADOOP_CLASS}/${INPUT_RANGE}"
fi

# Execute Hadoop command
hadoop jar ../output/HadoopWordCount.jar "$HADOOP_CLASS" "$INPUT_PATHS" "$OUTPUT_PATH"
exit $?