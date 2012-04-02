#!/bin/bash

metas=cayenne
metaport=20000
hostsfile=~/code/kfs-0.5/scripts/machines.txt
kfshome=~/code/kfs-0.5
tempfiles=/localfs/hamur/

# Clear old files
echo "Clearing old files..."
rm -f $tempfiles"/*"

# Start workdaemons
echo "Starting workdaemons..."
./run_workdaemon_local.sh &

sleep 10
#Start master
echo "Starting master..."
./run_master.sh &

#Start monitoring scripts
echo "Starting monitoring scripts..."
./mon.py `pgrep workdaemon` $tempfiles"/mon.out"

echo "Map Statistics" 
./avg_map_mem_2s.py $tempfiles"/mon.out"

