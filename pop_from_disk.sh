echo "Select * From Disks;" | sqlite3 test.db | sed -e 's/^[0-9]*|\(.*\)|\(.*\)|.*|.*$/ssh -n \2 -T ls \1/g' | while read line; do echo "$line"; eval $line;  echo " ------------- " ; done
