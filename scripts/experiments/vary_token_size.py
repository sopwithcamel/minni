import os,sys,subprocess

for token_size in [4096*4**x for x in range(6)]:
    num_tokens = int(token_size * 0.2)
    # Generate new config file
    print "Run: Token size: " + str(token_size) + ", max tokens: " + str(num_tokens) + ", run: 1"
    print "Generating config file"
    subprocess.call("../config/mod -minni__tbb__token_size " + str(token_size) + " -minni__tbb__max_keys_per_token " + str(num_tokens), shell=True)
    subprocess.call("cd ../monitoring/; ./run_minni_local.sh; cd -", shell=True)
    subprocess.call("sleep 5", shell=True)
