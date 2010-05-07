import random
import sys

(me,outfile, low, high, order) = sys.argv
f = open(outfile, 'w')
number = 2**int(order)
a = int(low)
b = int(high)
for i in xrange(0,number):
    N = random.randint(a,b)
    f.write(str(N) + " ")
