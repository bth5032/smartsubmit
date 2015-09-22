#echo "Select * From Disks;" | sqlite3 test.db | sed -e 's/^[0-9]*|\(.*\)|\(.*\)|.*|.*$/ssh -n \2 -T ls \1/g' | while read line; do echo "$line"; eval $line;  echo " ------------- " ; done

echo "Select * From Disks;" | sqlite3 test.db | sed -e 's/^[0-9]*|\(.*\)|\(.*\)|.*|.*$/ssh -n \2 -T ls \1/g' > tempfile


while read line
do 
	sample_dirs=`$line`
	if [[ -n "$sample_dirs" ]] 
	then
		for dirname in $sample_dirs
		do
			machine=`echo $line | cut -d ' ' -f 3`
			basedir=`echo $line | cut -d ' ' -f 6`
			com="ssh -n $machine -T ls $basedir$dirname/"
			sample_files=`$com`
			if [[ -n "$sample_files" ]]
			then
				for file in $sample_files
				do
					echo "INSERT INTO SampleFiles==> filename: $file, sample: $dirname, path: $basedir$dirname/, machine: $machine"
				done
			fi
		done
	fi
done < tempfile

rm tempfile