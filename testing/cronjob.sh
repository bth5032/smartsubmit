#!/bin/bash

pushd /root/ss_testing/
pwd
date

Message=""
Errors="false"


SS_CRON_OUTFILE="/root/ss_testing/output/"`date +%d%B%Y`".output"
WEBLOG_FILE="/root/ss_testing/weblog"

if [[ ! -f $WEBLOG_FILE ]]
then
	touch $WEBLOG_FILE
fi

if [[ ! -f $SS_CRON_OUTFILE ]]
then
	touch $SS_CRON_OUTFILE
fi

out=`timeout 15 /root/smartsubmit/smartsubmit_ctrl.py --list_samples -v5`

if [[ "$?" -eq 0 ]]
then
	date=`date`
	echo "$date: Server is alive" >> $WEBLOG_FILE
	#We have recieved the list of files from smartsubmit
	for line in `echo $out | sed -e "s/\[//g" -e "s/',//g" -e "s/u'//g" -e "s/'//g" -e "s/,//g" | tr ']' '\n' | sed -e "s/^ //g" -e "s/ /|/g"` 
	do 
		#each line is the row of the SampleFiles directory delimited by | 
		f=`echo $line | cut -d "|" -f 3,4,5,7 | sed -e "s/\(.*\)|\(.*\)|\(.*\)|\(.*\)/\4:\1\2:\3\2/g"`
		#Each f is a list of Machine|localPath|HadoopPath
		machine=`echo $f | cut -d ":" -f1`
		location=`echo $f | cut -d ":" -f2`
		hdloc=`echo $f | cut -d ":" -f3`
		echo "$machine:$location" >> $SS_CRON_OUTFILE
		ssh_output=`ssh $machine "cksum $location"`
		exit_code="$?"
		ret=`echo "$ssh_output" | cut -d ' ' -f1`
		lastcount=`sqlite3 filetest.db "SELECT WordCount FROM FileInfo WHERE HadoopPath='$hdloc'"`
		if [[ ! "$exit_code" -eq 0 ]]
		then
			sample=$(basename `dirname $location`)
			filename=`basename $location`
			Errors="true"
			#The file did not exist....
			Message+="FILE ERROR ==> Sample: $sample \t File: $filename \t Machine: $machine \t Exit Code: $exit_code. \n HadoopPath: $hdloc \n======================\n"
		elif [[ "$lastcount" == "$ret" ]]
		then
			echo "Counts Equal" >> $SS_CRON_OUTFILE
			sqlite3 filetest.db "UPDATE FileInfo SET LastCheckDate='$date' WHERE HadoopPath='$hdloc'"
		elif [[ -z "$lastcount" ]]
		then
			echo "No last count, inserting" >> $SS_CRON_OUTFILE
			sqlite3 filetest.db "INSERT INTO FileInfo(HadoopPath, WordCount, LastCheckDate) Values('$hdloc','$ret','$date')"
		else
			Errors="true"
			echo "Counts don't agree" >> $SS_CRON_OUTFILE
			LastCheckDate=`sqlite3 filetest.db "SELECT LastCheckDate FROM FileInfo WHERE HadoopPath='$hdloc'"`
			echo "Last count $LastCountDate -- $lastcount" >> $SS_CRON_OUTFILE
			echo "This count $ret" >> $SS_CRON_OUTFILE
			Message+="COUNT ERROR: Issue with file at hadoop location $hdloc \n  TODAY : $ret ==||== $LastCheckDate:  $lastcount.\n======================\n"
		fi
	done
else
	#There was an error, we never got a list of files from the server
	Errors="true"
	date=`date "+%F_%H:%M:%S"`
	echo "$date: Server may be down." >> $SS_CRON_OUTFILE
	Message+="TIMEOUT ERROR: Timeout server response at $date, please restart the server manually.\n======================\n"
fi

echo "=================================================\n" >> $SS_CRON_OUTFILE

if [[ "$Errors" == "true" ]]
then
	echo -e "$Message" | mailx -s "Errors" "bthashemi@ucsd.edu"
fi

popd
