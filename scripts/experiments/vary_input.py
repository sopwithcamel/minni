import os,sys,subprocess

buffer_size = 30*1024**2;
fanout = 8
for uniq in [10, 20, 30, 40, 50, 60, 70]:
    for rep in [int(100/uniq), int(1000/uniq)]:
        fil = str(uniq) + "mil" + str(rep) + "x" + "10b.txt"
        # Generate file
        subprocess.call("../inputgen/genr " + str(uniq * 1000000) + " " + str(rep) + " 7", shell=True)
        # Copy to kfs
        subprocess.call("cd ~/code/kfs-0.5/build/bin/tools; ./kfsshell -s cayenne -p 20000 -q rm dataset.txt; ./cptokfs -s cayenne -p 20000 -d /mnt/hamur/randinput.txt -k ./dataset.txt; cd -", shell=True)

        for (selected, so_name) in [("cbt", "/usr/local/lib/minni/wordcount.so"), ("sparsehash", "/usr/local/lib/minni/wcplain.so")]:
        # Generate new config file
            subprocess.call("../config/mod -minni__input_files dataset.txt " + " -minni__internal__selected " + selected + " -minni__so_name " + so_name + " -minni__internal__cbt__buffer_size " + str(buffer_size) + " -minni__internal__cbt__fanout " + str(fanout), shell=True)
            subprocess.call("cd ../monitoring/; ./run_minni_local.sh; cd -", shell=True)
            subprocess.call("sleep 5", shell=True)
