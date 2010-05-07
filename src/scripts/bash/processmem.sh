#!/bin/sh
# processmem - display memory claimed by a process
# gwild 03192004
#
if [ $# -lt 1 -o \( $# -gt 2 -a $# -lt 4 \) ]
then
  echo "Usage:"
  echo "processmem \"process\" \"ignore\""
  echo "Example:"
  echo "processmem rpc sh"
  exit 1
fi
echo " "

PROCESS=$1
IGNORE=$2

echo `UNIX95= ps -e -o vsz=Kbytes -o ruser -o pid,args=Command-Line | sort -rnk1 | grep -v Kbytes | grep $PROCESS| grep -v $IGNORE  | grep -v grep | grep -v $0`

echo `UNIX95= ps -e -o rsz=Kbytes -o ruser -o pid,args=Command-Line | sort -rnk1 | grep -v Kbytes | grep $PROCESS| grep -v $IGNORE  | grep -v grep | grep -v $0`

vsize=0
for sz in `UNIX95= ps -e -o vsz=Kbytes -o ruser -o pid,args=Command-Line | sort -rnk1 | grep -v Kbytes | grep $PROCESS | grep -v $IGNORE  | grep -v grep | grep -v $0 | awk '{print $1}'` 
do
	vsize=`expr $vsize + $sz`
done
#echo `expr $mps \* 4096`
echo "Virtual Memory claimed by $PROCESS: $vsize KBytes."

rssize=0
for sz in `UNIX95= ps -e -o rsz=Kbytes -o ruser -o pid,args=Command-Line | sort -rnk1 | grep -v Kbytes | grep $PROCESS | grep -v $IGNORE | grep -v grep | grep -v $0 | awk '{print $1}'` 
do
	rssize=`expr $rssize + $sz`
done

echo "Resident Memory claimed by $PROCESS: $rssize KBytes."

echo `date`
