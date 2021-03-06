#!/usr/bin/env bash

# Environment Variables set these to your correct settings
export GAME_LOGS="../../game_logs"
export LOG_OUTPUT="../../log_analysis/"
declare -a PACKAGES=("example" "porag" "russell")

# future variables
export OUTPUT_FILE=""
export INPUT_FILE=""

# specific to the organization of this project
export SOURCE_PATH="src/main/java/org/powertac/logtool"
export CLASS_NAME="org.powertac.logtool"
export CLASS_FILE=""

# handle compressed tars...
export IS_TGZ=false
export IS_TBZ=false
export CHECK_GZ=""
export CHECK_BZ=""

# Check for the right number of inputs...
if [ $# -lt 2 ]; then
	echo "Must run using './ltrunner.sh <class-name> <input-file> [output-file]'"
	exit 1
elif [ $# -gt 3 ]; then
	echo "Unknown input... args: ${#} run using './ltrunner.sh <class-name> <input-file> [output-file]'"
	exit 1
else
	# set up the class file
	for i in "${PACKAGES[@]}"; do
		if [ -f "${SOURCE_PATH}/${i}/${1}.java" ]; then
			CLASS_FILE=${CLASS_NAME}.${i}.${1}
		fi
	done
	if [ -z $CLASS_FILE ]; then
		# class was not found...
		echo "Unknown class"
		exit 2
	fi

	# set up the input file (we need to copy it to the running dir to work best...)
	# first we need to check for a tgz file
	CHECK_GZ=`expr "${2}" : '.*\(.tar.gz\)'`
	CHECK_BZ=`expr "${2}" : '.*\(.tar.bz2\)'`
	if [ $CHECK_GZ ]; then
		IS_TGZ=true
		# get the file name for the actual log.state
		export FILE_IN_QUESTION=`tar ztf ${GAME_LOGS}/${2} | grep -v 'init.state' | grep '\.state$'`
		# untar only that file, don't keep the log/ directory
		tar xzf ${GAME_LOGS}/${2} -C . --strip-components 1 $FILE_IN_QUESTION
		# strip off log/ from the filename
		INPUT_FILE="${FILE_IN_QUESTION#log/}"
	elif [ $CHECK_BZ ]; then
		IS_TBZ=true
		# get the file name for the actual log.state
		#export FILE_IN_QUESTION=`tar jtf ${GAME_LOGS}/${2} | grep -v 'init.state' | grep '\.state$'`
		# cluster configuration...
		export FILE_IN_QUESTION=`tar jtf ${GAME_LOGS}/${2} | grep -v 'init.state'  | grep -v 'boot.state' | grep '\.state$'`
		# untar only that file, don't keep the log/ directory
		tar xjf ${GAME_LOGS}/${2} -C . --strip-components 1 $FILE_IN_QUESTION
		# strip off log/ from the filename
		INPUT_FILE="${FILE_IN_QUESTION#log/}"
	else
		# copy the log file to home directory
		cp ${GAME_LOGS}/${2} `pwd`
		INPUT_FILE="${2}"
	fi

	# set up the output file...
	if [ $# -eq 2 ]; then
		# if not specified, name the output file <input>_<analysis>.csv without keeping the extension
		if [ "$IS_TGZ" = true ]; then
			OUTPUT_FILE="${2%%.tar.gz}_${1}.csv"
		elif [ "$IS_TBZ" = true ]; then
			OUTPUT_FILE="${2%%.tar.bz}_${1}.csv"
		else
			OUTPUT_FILE="${2%%.state}_${1}.csv"
		fi
	else
		OUTPUT_FILE=$3
	fi
fi

# build the package
#mvn clean test

# run the script
echo "mvn exec:exec -Dexec.args=\"${CLASS_FILE} ${INPUT_FILE} ${OUTPUT_FILE}\""
mvn exec:exec -Dexec.args="${CLASS_FILE} ${INPUT_FILE} ${OUTPUT_FILE}"

# clean up the running directory
# put the analysis where specified above
if [ -f $OUTPUT_FILE ]; then
	mv $OUTPUT_FILE $LOG_OUTPUT
fi
find . -name '*.csv' -exec mv {} ../../log_analysis/ \;
# remove the temp input file
rm "${INPUT_FILE}"
