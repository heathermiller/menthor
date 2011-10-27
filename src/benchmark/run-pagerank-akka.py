
# Script for benchmarking PageRank.
# command line args: [result file name] [input size]

import os
from time import time
import sys

out = file('bench/%s.bench' % sys.argv[1], 'w')
inputSize = sys.argv[2]
noOfRuns = 5
print "Running Akka"
print 'input size = %s' % inputSize

#javaargs="-Xms1G -Xmx8G"
javaargs="-Xmx4G -Xmx8G"
out.write("JAVA OPTS: "+javaargs+"\n")

#for numCores in [2] + range(3, 14, 2): # range(x,y,s) "from x to y-1 in steps of s"
for numCores in [2,3,4,5,6,7,8]:
    print "Running with %d cores..." % (numCores - 1)
    opts = javaargs + " -Dactors.corePoolSize=%d -Dactors.maxPoolSize=%d" % (numCores, numCores)
    os.putenv("JAVA_OPTS", opts)
    out.write("#cores "+str(numCores - 1)+"\t")
    tempTimes = []    

    # Run the benchmark several times and print the run times
    for run in range(noOfRuns):
        start = time()
        cmd = "time scala -cp bin:config:lib/akka-actor-1.1.jar menthor.akka.PageRank 30 data-large/ %s" % inputSize
        print 'running command: %s' % cmd

        os.system(cmd)
        elapsed = time() - start
        print elapsed
        out.write('\t%f' % elapsed)
        tempTimes.append(elapsed)
    
    # Calculate and print average run time
    sumT = 0
    for t in tempTimes:
        sumT = sumT + t
    avgT = sumT/noOfRuns
    out.write('\t:'+str(avgT)+'\n')
    
    
#call(["time", "scala", "-cp", "../classes", "processing.parallel.PageRank", "10", "../large-data/", "4000"])

