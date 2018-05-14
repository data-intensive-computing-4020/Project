import sys
import os

rowSteps = eval(sys.argv[1])
nodeSteps = eval(sys.argv[2])
generateMode = sys.argv[3]
benchmarkMode = sys.arv[4]


loop = 0
for row in rowSteps:
    if "M" in benchmarkMode:
        for node in nodeSteps:
            print("MPI row: {} node: {}".format(row,node))
            os.system("python controller.py {} {} M {} {}".format(row,node,generateMode,loop))
            loop=loop+1
    if "R" in benchmarkMode:
        print("MPI row: {}".format(row))
        os.system("python controller.py {} {} R {} {}".format(row, node, generateMode, loop))