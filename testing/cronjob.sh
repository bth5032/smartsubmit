#!/bin/bash

WC_PATH="/nfs-7/t2tas/test_executable"

pushd /tmp/ss_testing/

out=`ss_ctrl --list_samples -v5`
for line in `echo $out | sed -e "s/\[//g" -e "s/',//g" -e "s/u'//g" -e "s/'//g" -e "s/,//g" | tr ']' '\n' | sed -e "s/^ //g" -e "s/ /|/g"` 
do 
	for f in `echo $line | cut -d "|" -f 7,4,3 | sed -e "s/\(.*\)|\(.*\)|\(.*\)/\3:\1\2/g"`
	do
		machine=`echo $f | cut -d ":" -f1`
		location=`echo $f | cut -d ":" -f2`
		#hdloc=`echo $f | cut -d ":" -f3`
		ret=`ssh $machine "wc -l $location"`
		echo $ret
	done
done

popd