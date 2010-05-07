#!/bin/sh
# processmem - display memory claimed by a process
# gwild 03192004
#
if [ $# -lt 1 -o \( $# -gt 1 -a $# -lt 4 \) ]
then
  echo "Usage:"
  echo "processmem \"process\""
  echo "Example:"
  echo "processmem rpc"
  exit 1
fi
echo " "

PROCESS=$1

vsize=0
for sz in `UNIX95= ps -e -o vsz=Kbytes -o ruser -o pid,args=Command-Line | sort -rnk1 | grep -v Kbytes | grep $PROCESS | grep -v grep | grep -v $0 | awk '{print $1}'` 
do
	vsize=`expr $vsize + $sz`
done
#echo `expr $mps \* 4096`
echo "\nVirtual Memory claimed by $PROCESS: $vsize KBytes.\n"

rssize=0
for sz in `UNIX95= ps -e -o rsz=Kbytes -o ruser -o pid,args=Command-Line | sort -rnk1 | grep -v Kbytes | grep $PROCESS | grep -v grep | grep -v $0 | awk '{print $1}'` 
do
	rssize=`expr $rssize + $sz`
done

echo "Resident Memory claimed by $PROCESS: $rssize KBytes.\n"
