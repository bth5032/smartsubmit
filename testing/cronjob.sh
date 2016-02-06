#!/bin/bash

pushd /root/ss_testing/

out=`timeout 15 /root/smartsubmit/smartsubmit_ctrl.py --list_samples -v5`

pwd
date

Message=""
Errors="false"

if [[ "$?" -eq 0 ]]
then
	#We have recieved the list of files from smartsubmit
	for line in `echo $out | sed -e "s/\[//g" -e "s/',//g" -e "s/u'//g" -e "s/'//g" -e "s/,//g" | tr ']' '\n' | sed -e "s/^ //g" -e "s/ /|/g"` 
	do 
		#each line is the row of the SampleFiles directory delimited by | 
		for f in `echo $line | cut -d "|" -f 3,4,5,7 | sed -e "s/\(.*\)|\(.*\)|\(.*\)|\(.*\)/\4:\1\2:\3\2/g"`
		do
			#Each f is a list of  
			date=`date`
			machine=`echo $f | cut -d ":" -f1`
			location=`echo $f | cut -d ":" -f2`
			hdloc=`echo $f | cut -d ":" -f3`
			echo "$machine:$location"
			ssh_output=`ssh $machine "wc -l $location"`
			ret=`echo "$ssh_output" | cut -d ' ' -f1`
			lastcount=`sqlite3 filetest.db "SELECT WordCount FROM FileInfo WHERE HadoopPath='$hdloc'"`
			if [[ "$?" -eq 1 ]]
			then
				Errors="true"
				#The file did not exist....
				Message+="FILE ERROR: The file at $machine:$location was not readable by wc. \n HadoopPath: $hdloc \n======================"
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
				Message+="COUNT ERROR: Issue with file at hadoop location $hloc \n  TODAY : $ret ==||== $LastCheckDate:  $lastcount.\n======================"
			fi
		done
	done
else
	#There was an error, we never got a list of files from the server
	Errors="true"
	date=`date "+%F_%H:%M:%S"`
	Message+="TIMEOUT ERROR: Timeout server response at $date, please restart the server manually.\n======================"
fi

echo "================================================="

if [[ "$Errors" == "true" ]]
then
	echo -e "$Message" | mailx -s "Errors" "bthashemi@ucsd.edu"
fi

popd
