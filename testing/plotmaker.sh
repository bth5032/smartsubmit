#!/bin/bash
###########
# Makes root readable data files from the specified directory
###########

function makePlots {
	root -l -q -b  "rootplotmaker.c(\"${timing_dir}/runtimes.txt\", \"$TEST_RUN_NUM\")"
	mv runHist.png ${timing_dir}
}

function makeRunTimes {
	if [[ -z ${timing_dir+x} ]]
	then
		echo "timing_dir not set, don't know what file to read"
	else
		tf=${timing_dir}/timing_output.txt #timing output file
		echo "Pulling relevant info from $tf"
		sed -n '/^================$/,/^===========================================$/p' $tf | head -n-2 | tail -n+4 | cut -f 5,6,7 > ${timing_dir}/runtimes.tmp
		echo "Producing runtimes.txt"
		echo "REALTIME:USERTIME:SYSTIME" > ${timing_dir}/runtimes.txt
		while read -r a
		do 
			b=`echo $a | sed -e 's/\(.*\)m\(.*\)s \(.*\)m\(.*\)s \(.*\)m\(.*\)s$/\1*60+\2|\3*60+\4|\5*60+\6/'`
			rt=`echo $b | cut -d '|' -f 1 | bc` #real time
			ut=`echo $b | cut -d '|' -f 2 | bc` #user time
			st=`echo $b | cut -d '|' -f 3 | bc` #system time
			echo -e $rt"\t"$ut"\t"$st >> ${timing_dir}/runtimes.txt
		done < ${timing_dir}/runtimes.tmp
	fi
}

function makeAllRunTimes {
	#Check which folders hold the timing output files, and run makePlots on them

	for dname in `ls $ACTIVE_TESTING_DIR`
	do
		TEST_RUN_NUM=${dname#*_}
		if [[ -s ${ACTIVE_TESTING_DIR}/${dname}/batch/timing_output.txt ]]
		then
			echo "Making runtimes.txt"
			timing_dir=${ACTIVE_TESTING_DIR}/${dname}/batch/
			makeRunTimes
		fi

		if [[ -s ${ACTIVE_TESTING_DIR}/${dname}/ss/timing_output.txt ]]
		then
			echo "Making runtimes.txt"
			timing_dir=${ACTIVE_TESTING_DIR}/${dname}/ss/
			makeRunTimes
		fi 
	done
}

function makeAllPlots {
	#Check which folders hold the timing output files, and run makePlots on them

	for dname in `ls $ACTIVE_TESTING_DIR`
	do
		TEST_RUN_NUM=${dname#*_}
		if [[ -s ${ACTIVE_TESTING_DIR}/${dname}/batch/runtimes.txt ]]
		then
			echo "Making plots from: ${ACTIVE_TESTING_DIR}/${dname}/batch/timing_output.txt"
			timing_dir=${ACTIVE_TESTING_DIR}/${dname}/batch/
			makePlots
		fi

		if [[ -s ${ACTIVE_TESTING_DIR}/${dname}/ss/runtimes.txt ]]
		then
			echo "Making plots from: ${ACTIVE_TESTING_DIR}/${dname}/ss/timing_output.txt"		
			timing_dir=${ACTIVE_TESTING_DIR}/${dname}/ss/
			makePlots
		fi 
	done
}


#Check if active test directory exist, otherwise pull it in from the command line.

if [[ ! -d ACTIVE_TESTING_DIR ]]
then
	if [[ ! -d $1 ]]
	then 
		echo "No active testing directory given as command line arg"
	else
		ACTIVE_TESTING_DIR=$1
	fi
fi