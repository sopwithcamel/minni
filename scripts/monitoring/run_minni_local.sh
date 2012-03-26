#!/bin/bash

metas=cayenne
metaport=20000
hostsfile=~/code/kfs-0.5/scripts/machines.txt
kfshome=~/code/kfs-0.5

# Clear old files
echo "Clearing old files..."
rm -f /localfs/hamur/*

# Start workdaemons
cd ~/scripts
echo "Starting workdaemons..."
./run_workdaemon_local.sh &

#Start monitoring scripts
sleep 10
#echo "Starting monitoring scripts..."
~/scripts/mon.py `pgrep workdaemon` /localfs/hamur/mon.out &

#Start master
echo "Starting master..."
./run_master.sh

echo "Map Statistics" 
~/scripts/avg_map_mem_2s.py

