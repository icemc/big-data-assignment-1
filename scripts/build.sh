#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
# Source directory containing Java files
SRC_DIR="../src/main/java"
# Output directory for compiled classes and the JAR
OUTPUT_DIR="../output"
# Name of the final JAR file
JAR_NAME="HadoopWordCount.jar"
# --- End Configuration ---

# Ensure HADOOP_HOME is set
if [[ -z "$HADOOP_HOME" ]]; then
    echo "Error: HADOOP_HOME environment variable is not set." >&2
    exit 1
fi

# Check if HADOOP_HOME directory exists
if [[ ! -d "$HADOOP_HOME" ]]; then
    echo "Error: HADOOP_HOME directory '$HADOOP_HOME' not found." >&2
    exit 1
fi

# Construct classpath using HADOOP_HOME JARs
# Java's classpath wildcard needs to be quoted to prevent shell expansion
# Use colon ':' as the path separator in Linux
CLASSPATH="$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/mapreduce/*"

echo "[INFO] Using HADOOP_HOME: $HADOOP_HOME"
echo "[INFO] Using CLASSPATH wildcard pattern: $CLASSPATH" # Note: this shows the pattern, not expanded jars

# Create output directory if it doesn't exist (-p avoids error if it exists)
echo "[INFO] Creating output directory: $OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Compile Java files
echo "[INFO] Compiling Java source files from $SRC_DIR..."
# Check if there are any .java files to compile
if ! ls "$SRC_DIR"/*.java > /dev/null 2>&1; then
    echo "Warning: No .java files found in $SRC_DIR. Nothing to compile."
else
    # Use -cp for classpath, -d for output directory
    javac -cp "$CLASSPATH" "$SRC_DIR"/*.java -d "$OUTPUT_DIR"
    echo "[INFO] Compilation successful."
fi


# Change directory to output
echo "[INFO] Changing directory to $OUTPUT_DIR"
cd "$OUTPUT_DIR"

# Create JAR file from compiled classes in the current directory (.)
echo "[INFO] Creating JAR file: $JAR_NAME"
# Check if there are any .class files to package
if ! ls ./*.class > /dev/null 2>&1; then
     echo "Warning: No .class files found in $OUTPUT_DIR. Cannot create JAR."
     # Decide if this is an error or just a warning
     # exit 1 # Uncomment if an empty JAR is an error
else
    # -c create, -v verbose, -f specify filename
    jar -cvf "$JAR_NAME" *.class
    echo "[INFO] Build complete: $OUTPUT_DIR/$JAR_NAME"
fi


# Go back to the original directory (optional, but good practice)
# cd - > /dev/null

# Exit successfully
exit 0