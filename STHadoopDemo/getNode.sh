#!/bin/bash
unsort=$1
node=$2
echo "start"
#this commandline remove the douplicate values
sort $unsort | uniq | cat > $node
# this command line adds the header 
sed -i '1s/^/Node_id,latitude,longitude \n/' $node
# this command line remove the unsorted file
rm -rf $unsort
echo "done"
