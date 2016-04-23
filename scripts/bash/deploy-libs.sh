#!/bin/bash

# Copies all dependencies reported by Gradle to the running user's HDFS home
# directory under the folder "lib".

HDFS_LIB_DIR="lib";

SCRIPT_DIR="$(dirname $0)";
. $SCRIPT_DIR/util/functions.sh;

check-command hdfs;

echo "Checking for already existing lib dir";
if hdfs dfs -ls $HDFS_LIB_DIR > /dev/null; then
	echo "Removing existing lib dir";
	hdfs dfs -rm -r $HDFS_LIB_DIR;
fi;

echo "Creating new lib dir";
hdfs dfs -mkdir $HDFS_LIB_DIR;

echo "Computing classpath";
LIBS="$(echo $(get-gradle-classpath) | tr ':' ' ')";
LIBS="$LIBS $(get-jar)";

echo "Deploying libs to HDFS";
hdfs dfs -put $LIBS $HDFS_LIB_DIR;
