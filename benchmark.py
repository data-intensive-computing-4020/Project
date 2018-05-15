#   ____                  _                          _
# |  _ \                | |                        | |
# | |_) | ___ _ __   ___| |__  _ __ ___   __ _ _ __| | _____ _ __
# |  _ < / _ \ '_ \ / __| '_ \| '_ ` _ \ / _` | '__| |/ / _ \ '__|
# | |_) |  __/ | | | (__| | | | | | | | | (_| | |  |   <  __/ |
# |____/ \___|_| |_|\___|_| |_|_| |_| |_|\__,_|_|  |_|\_\___|_|
# This script calls both the generator and the controller scripts in order to automate and make the testing process simple
# This file only works in the context of running on the jaguar cluster as the machinefile is configured for that
# Input execution command: python benchmark.py "<Array of file sizes>" "<Array of number of processes>" equal MR
# equal specifies to use tables of equal sizes and MR states that the Mapreduce and MPI scripts should be called


import sys
import os

rowSteps = eval(sys.argv[1])
nodeSteps = eval(sys.argv[2])
generateMode = sys.argv[3]
benchmarkMode = sys.argv[4]


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
