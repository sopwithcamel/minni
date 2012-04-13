import os,sys,subprocess

buffer_size = 150*1024**2;
for fanout in [2**x for x in range(7)[2:]]:
# Generate new config file
    print "Run: CBT buffer size: " + str(buffer_size) + ", fan-out: " + str(fanout)
    subprocess.call("../config/mod " + "-minni__internal__cbt__buffer_size " + str(buffer_size) + " -minni__internal__cbt__fanout " + str(fanout), shell=True)
    subprocess.call("cd ../monitoring/; ./run_minni_local.sh; cd -", shell=True)
    subprocess.call("sleep 5", shell=True)
