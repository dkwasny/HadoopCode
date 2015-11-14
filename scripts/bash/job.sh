#!/bin/bash

# Executes an arbitrary class using the given input.
#
# The first parameter must be the class name you want to execute followed by
# however many parameters you need

BASE_DIR="$(dirname $0)/../..";
TARGET_DIR="$BASE_DIR/target";
POM_FILE="$BASE_DIR/pom.xml";

if [ ! -f "$POM_FILE" ]; then
	echo "ERROR: Could not find pom...ensure you are running this script from the correct location and compiled the program.";
	exit 1;
fi;

CLASS="$1";
shift;

# Update the jar just in case...remove if compile time is unruly (unlikely)
mvn -f $POM_FILE package;

export HADOOP_CLASSPATH="$(mvn -f $POM_FILE dependency:build-classpath | grep -v INFO)";
LIB_JARS="$(echo $HADOOP_CLASSPATH | tr ':' ',')";

hadoop jar $TARGET_DIR/*.jar $CLASS -libjars "$LIB_JARS" "$@";
