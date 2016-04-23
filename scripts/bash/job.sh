#!/bin/bash

# Executes an arbitrary class using the given input.
#
# The first parameter must be the class name you want to execute followed by
# however many parameters you need

SCRIPT_DIR="$(dirname $0)";
. $SCRIPT_DIR/util/functions.sh;

check-command hadoop;

CLASS="$1";
shift;

# Update the jar just in case...remove if compile time is unruly (unlikely)
execute-gradlew build;
JAR="$(get-jar)";

HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$(get-gradle-classpath)";
HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$JAR";
export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:/etc/hbase/conf";
hadoop jar "$JAR" "$CLASS" "$@";
