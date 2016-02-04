#!/bin/bash

WC_PATH="/nfs-7/t2tas/test_executable"

pushd /tmp/ss_testing/

out=`timeout 15 ss_ctrl --list_samples -v5`

if [[ "$?" -eq "0" ]]
then
	for line in `echo $out | sed -e "s/\[//g" -e "s/',//g" -e "s/u'//g" -e "s/'//g" -e "s/,//g" | tr ']' '\n' | sed -e "s/^ //g" -e "s/ /|/g"` 
	do 
		for f in `echo $line | cut -d "|" -f 7,4,3 | sed -e "s/\(.*\)|\(.*\)|\(.*\)/\3:\1\2/g"`
		do
			date=`date`
			machine=`echo $f | cut -d ":" -f1`
			location=`echo $f | cut -d ":" -f2`
			#hdloc=`echo $f | cut -d ":" -f3`
			ret=`ssh $machine "wc -l $location"`
			lastcount=`sqlite3 filetest.db "SELECT WordCount FROM FileInfo WHERE HadoopPath='$hdloc'"`
			if [[ "$lastcount" -eq "$ret" ]]
			then
				sqlite3 filetest.db "UPDATE FileInfo SET LastCheckDate='$date' WHERE HadoopPath='$hdloc'"
			elif [[ -z "$lastcount" ]]
				sqlite3 filetest.db "INSERT HadoopPath, WordCount, LastCheckDate INTO FileInfo Values('$hdloc','$ret','$date')"
			else
				echo "There may be an issue with the file at hadoop location $hloc\
				\
				At $date, the wordcount was $ret, but in the database the wordcount is $lastcount." | mailx -s "Possible issue with Smartsubmit file" "bthashemi@ucsd.edu"
			fi
		done
	done
else
	#There was an error, we never a list of files from the server
	
fi

popd