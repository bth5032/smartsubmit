#!/bin/bash
###########
# Makes root readable data files from the specified directory
###########

if [[ -z ${timing_dir+x} ]]
then
	echo "timing_dir not set, don't know what file to read"
else
	tf=${timing_dir}/timing_output.txt #timing output file
	line_num=`cat $tf | grep -n "^================$" | tail -n1 | cut -f 1 -d :` #get line where output per job starts
	tail -n +$(( $l + 1 )) | head -n-3 | cut -f 5,6,7 > ${timing_dir}/runtimes.tmp
	echo "REALTIME:USERTIME:SYSTIME" > ${timing_dir}/runtimes.txt
	while read -r a
	do 
		b=`echo $a | sed -e 's/\(.*\)m\(.*\)s \(.*\)m\(.*\)s \(.*\)m\(.*\)s$/\1+\2|\3+\4|\5+\6/'`
		rt=`echo $b | cut -d '|' -f 1 | bc` #real time
		ut=`echo $b | cut -d '|' -f 2 | bc` #user time
		st=`echo $b | cut -d '|' -f 3 | bc` #system time
		echo $rt"\t"$ut"\t"$st >> ${timing_dir}/runtimes.txt
	done < ${timing_dir}/runtimes.tmp
	
	#root -l rootplotmaker.c('${timing_dir}/runtimes.txt')
fi