import os,sys,subprocess

fanout = 8
for buffer_size in [60*1024**2*x for x in range(7)[3:]]:
# Generate new config file
    print "Run: CBT buffer size: " + str(buffer_size)
    subprocess.call("../config/mod " + "-minni__internal__cbt__buffer_size " + str(buffer_size) + " -minni__internal__cbt__fanout " + str(fanout), shell=True)
    subprocess.call("cd ../monitoring/; ./run_minni_local.sh; cd -", shell=True)
    subprocess.call("sleep 5", shell=True)
