#!/usr/bin/python
import glob, os.path, numpy

files = glob.glob("/localfs/hamur/map-bucket*")
size_list = []
for file in files:
  size_list.append(float(os.path.getsize(file)) / 1024**2)
print "Mean bucket size: ", numpy.mean(size_list)
print "Std. deviation: ", numpy.std(size_list)
hist, edges =  numpy.histogram(size_list)
print "Hist: ", hist
print "Bin edges: ", edges
  
