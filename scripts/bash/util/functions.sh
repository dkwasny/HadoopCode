# Checks if a command is available on this machine.
# If the command does not exist, the function exits with status 1.
function check-command() {
	if ! which "$1" > /dev/null; then
		echo "$1 command not found...exiting";
		exit 1;
	fi;
}

# Retrieves the full classpath for this project and sets it to
# PROJECT_CLASSPATH.
# The targeted pom file must be passed in as the first and only argument.
function get-maven-classpath() {
	if [ -z "$1" ]; then
		echo "No pom file provided...exiting";
		exit 1;
	fi;

	OUTPUT_FILE=$(mktemp);
	mvn -f $1 dependency:build-classpath -Dmdep.outputFile=$OUTPUT_FILE;
	PROJECT_CLASSPATH="$(cat $OUTPUT_FILE)";
	rm $OUTPUT_FILE;
}
