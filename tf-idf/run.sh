#!/usr/bin/env bash

HADOOP_CMD="/usr/local/src/hadoop-2.6.1/bin/hadoop"
STREAM_JAR_PATH="/usr/local/src/hadoop-2.6.1/share/hadoop/tools/lib/hadoop-streaming-2.6.1.jar"

INPUT_FILE_PATH_1="/tfidf_input.data"
OUTPUT_PATH="/tfidf_output"

$HADOOP_CMD fs -rmr -skipTrash $OUTPUT_PATH

# Step 1.
$HADOOP_CMD jar $STREAM_JAR_PATH \
    -input $INPUT_FILE_PATH_1 \
    -output $OUTPUT_PATH \
    -mapper "python map.py" \
    -reducer "python red.py" \
    -file ./map.py \
    -file ./red.py
