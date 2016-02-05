#!/bin/bash

pushd /root/ss_testing/

out=`timeout 15 /root/smartsubmit/smartsubmit_ctrl.py --list_samples -v5`

if [[ "$?" -eq "0" ]]
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
			#hdloc=`echo $f | cut -d ":" -f3`
			ret=`ssh $machine "wc -l $location"`
			lastcount=`sqlite3 filetest.db "SELECT WordCount FROM FileInfo WHERE HadoopPath='$hdloc'"`
			if [[ "$lastcount" -eq "$ret" ]]
			then
				sqlite3 filetest.db "UPDATE FileInfo SET LastCheckDate='$date' WHERE HadoopPath='$hdloc'"
			elif [[ -z "$lastcount" ]]
				sqlite3 filetest.db "INSERT HadoopPath, WordCount, LastCheckDate INTO FileInfo Values('$hdloc','$ret','$date')"
			else
				LastCheckDate=`sqlite3 filetest.db "SELECT LastCheckDate FROM FileInfo WHERE HadoopPath=$hdloc"`
				echo "Issue with file at hadoop location $hloc\
\
------- Date -- Wordcount\
TODAY : $date -- $ret \
OLD   : $LastCheckDate $lastcount." | mailx -s "file error" "bthashemi@ucsd.edu"
			fi
		done
	done
else
	#There was an error, we never got a list of files from the server
	date=`date "+%F_%H:%M:%S"`
	echo "Timeout server response at $date, please restart the server manually." | mailx -s "Possible issue with Smartsubmit server" "bthashemi@ucsd.edu"
	#nohup python /root/smartsubmit/smartsubmit_daemon.py & > "stdout_$date.output"
fi

popd