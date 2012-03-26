#!/usr/bin/python
import glob, os.path, os, numpy

file1 = open("/localfs/hamur/mon.out")
mem = []
cpu = []
for line in file1:
  line = line.split(",")
  mem.append(float(line[0]))
  cpu.append(float(line[1]))
print "Map memory: ", numpy.mean(mem)
print "Map CPU: ", numpy.mean(cpu)

