import glob, os.path, os, numpy, sys

tim_file = open("/localfs/hamur/timelog")

i = 0
for line in tim_file:
  if i == 0:
    map_start = long(line.split(" ")[-1][:-1])
  elif i == 1:
    st1_stop = long(line.split(" ")[-1][:-1])
  elif i == 2:
    map_stop = long(line.split(" ")[-1][:-1]) 
  else:
    break
  i = i + 1
st1_time = st1_stop - map_start
st2_time = map_stop - st1_stop
map_time = map_stop - map_start
print "Map time: ", map_time
print "Stage 1 time: ", st1_time
print "Stage 2 time: ", st2_time

os.system("head -n " + str(map_time) + " " + sys.argv[1] + " | head -n " + str(st1_time) + " > /localfs/hamur/stage1_tim.temp")
os.system("head -n " + str(map_time) + " " + sys.argv[1] + " | tail -n " + str(st2_time) + " > /localfs/hamur/stage2_tim.temp")
os.system("head -n " + str(map_time) + " " + sys.argv[1] + " > /localfs/hamur/map_tim.temp")
tim_file.close()

file1 = open("/localfs/hamur/map_tim.temp")
mem = []
cpu = []
for line in file1:
  line = line.split(",")
  mem.append(float(line[0]))
  cpu.append(float(line[1]))
print "Map memory: ", numpy.mean(mem)
print "Map CPU: ", numpy.mean(cpu)

file2 = open("/localfs/hamur/stage1_tim.temp")
mem = []
disk_read = []
disk_write = []
for line in file2:
  line = line.split(",")
  mem.append(float(line[0]))
  disk_read.append(float(line[2]))
  disk_write.append(float(line[3]))
print "Stage 1 memory: ", numpy.mean(mem)
print "Stage 1 disk read: ", numpy.mean(disk_read)
print "Stage 1 disk write: ", numpy.mean(disk_write)

file3 = open("/localfs/hamur/stage2_tim.temp")
mem = []
for line in file3:
  line = line.split(",")
  mem.append(float(line[0]))
print "Stage 2 memory: ", numpy.mean(mem)
