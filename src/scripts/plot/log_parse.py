import sys
import re

(me, infile,outfile) = sys.argv

FH = open(infile,'r')
File = FH.read()
FH.close()
File = File.split('\n')

virtual = re.compile("Virtual.*?(\d+)")
physical = re.compile("Resident.*?(\d+)")
time = re.compile(".*?(\d+):(\d+):(\d+)")

Data = [];
t = 0;
read_something = False;
for l in xrange(0, len(File)):
    v = ""
    r = ""

    # Check the virtual
    result = virtual.search(File[l])
    if(result != None):
        v = result.group(1)
    else:
        continue

    result = physical.search(File[l+1])
    if(result != None):
        r = result.group(1)
    else:
        print "PARSE ERROR\n"

    #print(line)
    Data.append([str(t),r,v]);
    if(r == "0" and v == "0"):
        if(read_something):
            break;
        else:
            continue;
    if(r == "" or v == ""):
        print "Parse error"
        break;
    t +=1
    read_something = True

FH.close();
OH = open(outfile,'w')
OH.write('\n'.join(map(lambda x: ','.join(x), Data)))
OH.close()

