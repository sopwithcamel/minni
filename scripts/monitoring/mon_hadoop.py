#!/usr/bin/python
import sys, os, time, signal

pgsize = 4096

if len(sys.argv) == 2:
  outfile = sys.argv[1]
else:
  outfile = "/localfs/hamur/mon_hadoop.out"

fil = open(outfile, "w")

def sigint_handler(signum, frame):
  fil.close()
  sys.exit(0)

inoct_prev = -1
outoct_prev = -1

signal.signal(signal.SIGINT, sigint_handler)

mon_off_threshold = 100

while True:
  pids = []
  hadoop = os.popen("jps");
  jobs = hadoop.read()
  for line in jobs.split("\n")[:-1]:
    pname = line.split(" ")
    if pname[-1].startswith("Child") or pname[-1].startswith("Task"):
#      print "Adding", pname[0]
      pids.append(pname[0])
  if len(pids) == 0:
    time.sleep(1)
    continue
    
  write_str = ""
  mem = 0
  inoct = 0
  outoct = 0
  for pid in pids:
    if os.path.exists("/proc/" + str(pid)):
      child_mem = os.popen("cat /proc/" + str(pid) + "/statm")
      child_net = os.popen("cat /proc/" + str(pid) + "/net/netstat | grep IpExt")

      mem_str = child_mem.read()
      mem = mem + float(mem_str.split(" ")[1]) * 4 / 1024

      nets = child_net.read().split("\n")[1].split(" ")
      inoct = inoct + int(nets[7])
      outoct = outoct + int(nets[8])

  write_str = write_str + str(mem)
  if (inoct_prev > 0):
    write_str = write_str + ", " + str(float(inoct - inoct_prev) / 1024**2)
    write_str = write_str + ", " + str(float(outoct - outoct_prev) / 1024**2)
    fil.writelines(write_str + "\n")
  inoct_prev = inoct
  outoct_prev = outoct
    
  child_mem.close()
  child_net.close()
  time.sleep(1)
