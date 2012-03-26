#!/usr/bin/python
import sys, os, time, signal

pgsize = 4096

if len(sys.argv) < 2:
  print "What program should I monitor?"
  sys.exit(0)

if len(sys.argv) == 3:
  outfile = sys.argv[2]
else:
  outfile = "/localfs/hamur/mon.out"

pid = int(sys.argv[1])
fil = open(outfile, "w")

def sigint_handler(signum, frame):
  fil.close()
  sys.exit(0)

inoct_prev = -1
outoct_prev = -1

signal.signal(signal.SIGINT, sigint_handler)

while True:
    mem = 0
    write_str = ""

    nsort = os.popen("pgrep nsort")
    nsort_pid = nsort.read()[:-1] # Get rid of the trailing newline
    if nsort_pid:
      child_mem = os.popen("cat /proc/" + str(nsort_pid) + "/statm")
      nsort_mem = child_mem.read()
      mem = mem + float(nsort_mem.split(" ")[1]) * 4 / 1024
    
    if not os.path.exists("/proc/" + str(pid)):
      fil.close()
      sys.exit(0)

    child_mem = os.popen("cat /proc/" + str(pid) + "/statm")
    mem_str = child_mem.read()
    mem = mem + float(mem_str.split(" ")[1]) * 4 / 1024
   
    write_str = write_str + str(mem)

    child_net = os.popen("cat /proc/" + str(pid) + "/net/netstat | grep IpExt")
    nets = child_net.read().split("\n")[1].split(" ")
    inoct = int(nets[7])
    outoct = int(nets[8])

    if (inoct_prev > 0):
      write_str = write_str + ", " + str(float(inoct - inoct_prev) / 1024**2)
      write_str = write_str + ", " + str(float(outoct - outoct_prev) / 1024**2)
      fil.writelines(write_str + "\n")
    inoct_prev = inoct
    outoct_prev = outoct
    
    child_mem.close()
    child_net.close()
    time.sleep(1)
