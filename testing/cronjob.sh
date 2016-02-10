#!/bin/bash

pushd /root/ss_testing/
pwd
date

Message=""
Errors="false"

if [[ ! -f "/root/ss_testing/weblog" ]]
then
	touch /root/ss_testing/weblog
fi

out=`timeout 15 /root/smartsubmit/smartsubmit_ctrl.py --list_samples -v5`

if [[ "$?" -eq 0 ]]
then
	date=`date`
	echo "$date: Server is alive" >> /root/ss_testing/weblog
	#We have recieved the list of files from smartsubmit
	for line in `echo $out | sed -e "s/\[//g" -e "s/',//g" -e "s/u'//g" -e "s/'//g" -e "s/,//g" | tr ']' '\n' | sed -e "s/^ //g" -e "s/ /|/g"` 
	do 
		#each line is the row of the SampleFiles directory delimited by | 
		f=`echo $line | cut -d "|" -f 3,4,5,7 | sed -e "s/\(.*\)|\(.*\)|\(.*\)|\(.*\)/\4:\1\2:\3\2/g"`
		#Each f is a list of  
		machine=`echo $f | cut -d ":" -f1`
		location=`echo $f | cut -d ":" -f2`
		hdloc=`echo $f | cut -d ":" -f3`
		echo "$machine:$location"
		ssh_output=`ssh $machine "cksum $location"`
		$exit_code="$?"
		ret=`echo "$ssh_output" | cut -d ' ' -f1`
		lastcount=`sqlite3 filetest.db "SELECT WordCount FROM FileInfo WHERE HadoopPath='$hdloc'"`
		if [[ "$exit_code" -eq 1 ]]
		then
			Errors="true"
			#The file did not exist....
			Message+="FILE ERROR: The file at $machine:$location was not readable by cksum. \n HadoopPath: $hdloc \n======================"
		elif [[ "$lastcount" == "$ret" ]]
		then
			echo "Counts Equal"
			sqlite3 filetest.db "UPDATE FileInfo SET LastCheckDate='$date' WHERE HadoopPath='$hdloc'"
		elif [[ -z "$lastcount" ]]
		then
			echo "No last count, inserting"
			sqlite3 filetest.db "INSERT INTO FileInfo(HadoopPath, WordCount, LastCheckDate) Values('$hdloc','$ret','$date')"
		else
			Errors="true"
			echo "Counts don't agree"
			LastCheckDate=`sqlite3 filetest.db "SELECT LastCheckDate FROM FileInfo WHERE HadoopPath='$hdloc'"`
			echo "Last count $LastCountDate -- $lastcount"
			echo "This count $ret"
			Message+="COUNT ERROR: Issue with file at hadoop location $hdloc \n  TODAY : $ret ==||== $LastCheckDate:  $lastcount.\n======================"
		fi
	done
else
	#There was an error, we never got a list of files from the server
	Errors="true"
	date=`date "+%F_%H:%M:%S"`
	echo "$date: Server may be down."
	Message+="TIMEOUT ERROR: Timeout server response at $date, please restart the server manually.\n======================"
fi

echo "================================================="

if [[ "$Errors" == "true" ]]
then
	echo -e "$Message" | mailx -s "Errors" "bthashemi@ucsd.edu"
fi

popd
