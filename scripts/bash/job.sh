#!/bin/bash

# Executes an arbitrary class using the given input.
#
# The first parameter must be the class name you want to execute followed by
# however many parameters you need

SCRIPT_DIR="$(dirname $0)";
. $SCRIPT_DIR/util/set-pom.sh;
. $SCRIPT_DIR/util/check-command.sh;

check-command hadoop;

CLASS="$1";
shift;

# Update the jar just in case...remove if compile time is unruly (unlikely)
mvn -f $POM_FILE package;

export HADOOP_CLASSPATH="$(mvn -f $POM_FILE dependency:build-classpath | grep -v INFO)";
hadoop jar $TARGET_DIR/*.jar $CLASS "$@";
