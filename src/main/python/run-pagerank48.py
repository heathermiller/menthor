
# Script for benchmarking PageRank.
# command line args: [result file name] [input size]

import os
from time import time
import sys

out = file('%s.txt' % sys.argv[1], 'w')
inputSize = sys.argv[2]
print 'input size = %s' % inputSize

for numCores in [2] + range(3, 50, 2):
    print "Running with %d cores..." % (numCores - 1)
    opts = "-Xmx8G -Dactors.corePoolSize=%d -Dactors.maxPoolSize=%d" % (numCores, numCores)
    os.putenv("JAVA_OPTS", opts)
    
    for run in range(5):
        start = time()
        cmd = "time scala -cp ../classes-novolatile processing.parallel.PageRank 30 ../large-data/ %s" % inputSize
        print 'running command: %s' % cmd
        os.system(cmd)
        elapsed = time() - start
        print elapsed
        out.write('\t%f' % elapsed)
    
    out.write('\n')
    
    
#call(["time", "scala", "-cp", "../classes", "processing.parallel.PageRank", "10", "../large-data/", "4000"])

