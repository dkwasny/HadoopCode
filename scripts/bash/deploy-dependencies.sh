#!/bin/bash

HDFS_DEPENDENCY_DIR="dependencies";

SCRIPT_DIR="$(dirname $0)";
. $SCRIPT_DIR/util/set-pom.sh;
. $SCRIPT_DIR/util/check-command.sh;

check-command hdfs;

# Remove the existing dependency directory 
if hdfs dfs -ls $HDFS_DEPENDENCY_DIR > /dev/null; then
	echo hdfs dfs -rm -r $HDFS_DEPENDENCY_DIR;
fi;

echo hdfs dfs -mkdir $HDFS_DEPENDENCY_DIR;

DEPENDENCY_JARS="$(mvn -f $POM_FILE dependency:build-classpath | grep -v INFO | tr ':' ' ')"
echo hdfs dfs -put $DEPENDENCY_JARS $HDFS_DEPENDENCY_DIR;
