#!/usr/bin/env bash

# Environment Variables set these to your correct settings
export GAME_LOGS="../../game_logs"
declare -a PACKAGES=("example" "porag" "russell")

export ITERATION=".runiter"
export ITER_NUM=0
export OUTPUT_FILE="analysis_output"
export INPUT_FILE=""
export CLASS_PATH="src/main/java/org/powertac/logtool"
export CLASS_NAME="org.powertac.logtool"
export CLASS_FILE=""


# Check for the right number of inputs...
if [ $# -lt 2 ]; then
	echo "Must run using './ltrunner.sh <class-name> <input-file> [output-file]'"
	exit 1
elif [ $# -gt 3 ]; then
	echo "Unknown input... args: ${#} run using './ltrunner.sh <class-name> <input-file> [output-file]'"
	exit 1
else
	# set up the output file...
	if [ $# -eq 2 ]; then
		if [ -f $ITERATION ]; then
			ITER_NUM=$(<${ITERATION})
		fi
		OUTPUT_FILE="${OUTPUT_FILE}_${ITER_NUM}.csv"
		((ITER_NUM++))
		echo $ITER_NUM > $ITERATION
	else
		OUTPUT_FILE=$3
	fi

	# set up the input file
	INPUT_FILE="${GAME_LOGS}/${2}"

	# set up the class file
	for i in "${PACKAGES[@]}";
	do
		if [ -f "${CLASS_PATH}/${i}/${1}.java" ]; then
			CLASS_FILE=${CLASS_NAME}.${i}.${1}
		fi
	done
	if [ -z $CLASS_FILE ]; then
		echo "Unknown class"
		exit 2
	fi
fi
# build the package
mvn clean test

# run the script
mvn exec:exec -Dexec.args="${CLASS_FILE} ${INPUT_FILE} ${OUTPUT_FILE}"
