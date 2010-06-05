#!/bin/bash
rm hadoop.log
while [ 1 ]
do
	./processmem.sh mapred jobtracker >> hadoop.log
	sleep 10
done
