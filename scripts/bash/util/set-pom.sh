# A silly hardcoded script that finds this project's pom file.
# The value is ultimately set to $POM_FILE.

BASE_DIR="$(dirname $0)/../..";
TARGET_DIR="$BASE_DIR/target";
POM_FILE="$BASE_DIR/pom.xml";

if [ ! -f "$POM_FILE" ]; then
	echo "ERROR: Could not find pom...ensure you are running this script from the correct location and compiled the program.";
	exit 1;
fi;
