
# Script for benchmarking PageRank.
# command line args: [result file name] [input size]

import os
from time import time
import sys

inputSize = sys.argv[1]
parLimit = sys.argv[2]
noOfRuns = 5
print "Running..."
print 'input size = %s' % inputSize
print 'parallelism limit = %s' % parLimit

opts = "-Xmx4G -Xmx8G"
os.putenv("JAVA_OPTS", opts)

# Run the benchmark several times and print the run times
for run in range(noOfRuns):
	cmd = "time scala -cp bin:config:lib/akka-actor-1.1.jar menthor.akka.PageRank 30 data-large/ %s %s" % (inputSize, parLimit)
	print 'running command: %s' % cmd

	os.system(cmd)
	print 'done'
    

