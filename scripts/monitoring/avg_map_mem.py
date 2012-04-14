#!/usr/bin/python
import glob, os.path, os, numpy, sys
file1 = open(sys.argv[1])
mem = []
cpu = []
disku = []
for line in file1:
  line = line.split(",")
  mem.append(float(line[0]))
  cpu.append(float(line[1]))
print "Map memory: ", numpy.mean(mem)
print "Map CPU: ", numpy.mean(cpu)
