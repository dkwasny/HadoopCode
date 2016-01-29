# Checks if a command is available on this machine.
# If the command does not exist, the function exits with status 1.

function check-command() {
	if ! which "$1" > /dev/null; then
		echo "$1 command not found...exiting";
		exit 1;
	fi;
}
