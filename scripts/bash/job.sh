#!/bin/bash

# Executes an arbitrary class using the given input.
#
# The first parameter must be the class name you want to execute followed by
# however many parameters you need

SCRIPT_DIR="$(dirname $0)";
. $SCRIPT_DIR/util/set-pom.sh;
. $SCRIPT_DIR/util/functions.sh;

check-command mvn;
check-command hadoop;

CLASS="$1";
shift;

# Update the jar just in case...remove if compile time is unruly (unlikely)
mvn -f $POM_FILE package;

get-maven-classpath $POM_FILE;
hadoop jar $TARGET_DIR/*.jar $CLASS $PROJECT_CLASSPATH "$@";
