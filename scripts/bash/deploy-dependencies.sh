#!/bin/bash

HDFS_DEPENDENCY_DIR="dependencies";

SCRIPT_DIR="$(dirname $0)";
. $SCRIPT_DIR/util/set-pom.sh;
. $SCRIPT_DIR/util/functions.sh;

check-command mvn;
check-command hdfs;

# Remove the existing dependency directory 
if hdfs dfs -ls $HDFS_DEPENDENCY_DIR > /dev/null; then
	echo "Removing existing dependency dir";
	hdfs dfs -rm -r $HDFS_DEPENDENCY_DIR;
fi;

echo "Creating new dependency dir";
hdfs dfs -mkdir $HDFS_DEPENDENCY_DIR;

echo "Retrieving list of dependencies from Maven";
get-maven-classpath $POM_FILE;
DEPENDENCY_JARS="$(echo $PROJECT_CLASSPATH | tr ':' ' ')";

echo "Putting all dependencies on HDFS";
hdfs dfs -put $DEPENDENCY_JARS $HDFS_DEPENDENCY_DIR;
