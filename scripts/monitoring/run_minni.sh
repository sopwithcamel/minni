#!/bin/bash
## Find out how much reduction we can obtain when we increase the number of keys

metas=fawn122
metaport=20000
hostsfile=~/kfs-0.5/scripts/machines.txt
kfshome=~/kfs-0.5
outfile=~/scripts/hashsize.out

# CLear output file
echo "" > $outfile

# Clear old files
echo "Clearing old files..."
parallel-ssh -i -h $hostsfile "rm -f /localfs/hamur/*"

# Start workdaemons
cd ~/scripts
echo "Starting workdaemons..."
./run_workdaemons.sh &

#Start monitoring scripts
sleep 10
#echo "Starting monitoring scripts..."
#parallel-ssh -i -h $hostsfile '~/scripts/mon.py `pgrep workdaemon` /localfs/hamur/mon.out &'

#Start master
echo "Starting master..."
cd ~/minni/src/master/
./test > /localfs/hamur/master_output

echo "Map Statistics" 
parallel-ssh -i -h $hostsfile '~/scripts/avg_map_mem_3s.py' > /localfs/hamur/wkstats
~/scripts/process_stats.py /localfs/hamur/wkstats

