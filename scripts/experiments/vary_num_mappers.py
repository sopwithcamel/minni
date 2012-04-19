import os,sys,subprocess

buffer_size = 30*1024**2;
fanout = 8
uniq = 30
rep = 33
fil = str(uniq) + "mil" + str(rep) + "x" + "10b.txt"
selected = "cbt"
so_name = "/usr/local/lib/minni/wordcount.so"
# Generate file
subprocess.call("../inputgen/genr " + str(uniq * 1000000) + " " + str(rep) + " 7", shell=True)
# Copy to kfs
subprocess.call("cd ~/code/kfs-0.5/build/bin/tools; ./kfsshell -s cayenne -p 20000 -q rm dataset.txt; ./cptokfs -s cayenne -p 20000 -d /mnt/hamur/randinput.txt -k ./dataset.txt; cd -", shell=True)
# Generate new config file
for num_maps in range(6)[1:]
    subprocess.call("../config/mod -minni__input_files dataset.txt " + " -minni__internal__selected " + selected + " -minni__so_name " + so_name + " -minni__internal__cbt__buffer_size " + str(buffer_size) + " -minni__internal__cbt__fanout " + str(fanout) " -minni__max_maps " + str(num_maps), shell=True)
    subprocess.call("cd ../monitoring/; ./run_minni_local.sh; cd -", shell=True)
    subprocess.call("sleep 5", shell=True)
