# Checks if a command is available on this machine.
# If the command does not exist, the function exits with status 1.
function check-command() {
	if ! which "$1" > /dev/null; then
		echo "ERROR: $1 command not found...exiting" 1>&2;
		exit 1;
	fi;
}

# Echos the path of a file if it exists
function get-file() {
	local FILE="$1";
	if [ ! -f "$FILE" ]; then
		echo "ERROR: Could not find $FILE" 1>&2;
		exit 1;
	fi;
	echo "$FILE";
}

# Echos the jar compiled by Gradle
function get-jar() {
	echo "$(get-file $(dirname $0)/../../build/libs/*jar)";
}

# Execute gradlew using the supplied arguments
function execute-gradlew() {
	local SCRIPT_DIR="$(dirname $0)";
	echo "$(cd $SCRIPT_DIR/../../; ./gradlew $@)";
}

# Echos the runtime classpath as reported by Gradle
function get-gradle-classpath() {
	echo "$(execute-gradlew printClasspath | grep '^Classpath:' | sed 's/^Classpath://')";
}
