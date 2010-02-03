#!/bin/bash
#
# This script copies pubsub entries into simplequeue. It can work from a
# file (see $FILE) or stdin. Be sure to set the location of your simpleq
# server if it is not using standard host/port.
#
# Example:
#
#    pubsub-reader -e -u 'http://pubsub.host:port' | ./pubsub2simplequeue.sh
#

#
# BEGIN user definable properties
#

FILE=""
SIMPLEQ="http://localhost:8080"
 
#
# END user definable properties
#


# User define Function (UDF)
processLine(){
  line="$@" # get all args
  #  just echo them, but you may need to customize it according to your need
  # for example, F1 will store first field of $line, see readline2 script
  # for more examples
  # F1=$(echo $line | awk '{ print $1 }')
  #echo $line
  curl $SIMPLEQ"/put?data="$line
}
 
if [ "$1" == "" ]; then
   FILE="/dev/stdin"
else
   FILE="$1"
   # make sure file exist and readable
   if [ ! -f $FILE ]; then
  	echo "$FILE : does not exists"
  	exit 1
   elif [ ! -r $FILE ]; then
  	echo "$FILE: can not read"
  	exit 2
   fi
fi
# read $FILE using the file descriptors
 
# Set loop separator to end of line
BAKIFS=$IFS
IFS=$(echo -en "\n\b")
exec 3<&0
exec 0<"$FILE"
while read -r line
do
	processLine $line
done
exec 0<&3
 
# restore $IFS which was used to determine what the field separators are
IFS=$BAKIFS
exit 0
