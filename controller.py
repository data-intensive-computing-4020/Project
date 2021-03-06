#    _____            _             _ _
#  / ____|          | |           | | |
# | |     ___  _ __ | |_ _ __ ___ | | | ___ _ __
# | |    / _ \| '_ \| __| '__/ _ \| | |/ _ \ '__|
# | |___| (_) | | | | |_| | | (_) | | |  __/ |
#  \_____\___/|_| |_|\__|_|  \___/|_|_|\___|_|

# Script to run all benchmarks in turn and print results. Script starts by generating data of specified size, calling
# naive, MPI then mapreduce. lastly, the results are checked for integrity with the resultCheck.py script
# This script is called by controller.py and is not required to be run independently
import sys
import os
from colours import bcolors

if (len(sys.argv) != 6):
    print(
        bcolors.FAIL + "Incorrect number of arguments provided. First argument corresponds to number of rows to generate in table and second corresponds to the number of nodes to run the cluster on" + bcolors.ENDC)
    sys.exit()

rows = sys.argv[1]
nodes = sys.argv[2]
groupToRun = (sys.argv[3]).upper()
generatormode = sys.argv[4]
loopNumber = int(sys.argv[5])





if loopNumber == 0:
    # Input generation
    print(bcolors.UNDERLINE + "Generating tables..." + bcolors.ENDC)
    if int(os.system("python generator.py {} table1.json table2.json {}".format(rows,generatormode))) != 0:
        print(bcolors.FAIL + "Tables generated failed" + bcolors.ENDC)
        sys.exit(1)
    else:
        print(bcolors.OKBLUE + "Tables generated successfully\n" + bcolors.ENDC)

# Naive join
if "N" in groupToRun:
    print(bcolors.UNDERLINE + "Joining naively..." + bcolors.ENDC)
    if int(os.system(
            "python Naive.py table1.json 0 table2.json 0 NaiveJoin_result_R{}_N{}.json Benchmark2_R{}_{}.txt".format(rows,nodes,rows,generatormode))) != 0:
        print(bcolors.FAIL + "Naive join failed" + bcolors.ENDC)
        sys.exit(1)
    else:
        print(bcolors.OKBLUE + "Naive join successfully\n" + bcolors.ENDC)


# MPI join
if "M" in groupToRun:
    print(bcolors.UNDERLINE + "Joining over MPI cluster..." + bcolors.ENDC)
    if int(os.system(
            "mpirun -n {} -machinefile machinefile python MPIJoin.py table1.json 0 table2.json 0 MPIJoin_result_R{}_N{}.json Benchmark2_R{}_{}.txt".format(nodes,rows,nodes,rows,generatormode))) != 0:
        print(bcolors.FAIL + "MPI cluster join failed" + bcolors.ENDC)
        sys.exit(1)
    else:
        print(bcolors.OKBLUE + "MPI cluster join successfully\n" + bcolors.ENDC)

# MRJoin
if "R" in groupToRun:
    print(bcolors.UNDERLINE + "Joining over MRJoin..." + bcolors.ENDC)
    if int(os.system(
            "python MRJoin.py table1.json 0 table2.json 0 MRJoin_result_R{}_N{}.json Benchmark2_R{}_{}.txt".format(rows,nodes,rows,generatormode))) != 0:
        print(bcolors.FAIL + "MPI cluster join failed" + bcolors.ENDC)
        sys.exit(1)
    else:
        print(bcolors.OKBLUE + "MPI cluster join successfully\n" + bcolors.ENDC)

# results correctness check
if "MRN" in groupToRun:
    print(bcolors.UNDERLINE + "Checking results for correctness..." + bcolors.ENDC)
    if int(os.system(
            "python resultCheck.py NaiveJoin_result_R{}_N{}.json 0 MPIJoin_result_R{}_N{}.json 0 MRJoin_result_R{}_N{}.json 0".format(rows,nodes,rows,nodes,rows,nodes))) != 0:
        print(bcolors.FAIL + "results check failed" + bcolors.ENDC)
        sys.exit(1)
    else:
        print(bcolors.OKBLUE + "Results check finished" + bcolors.ENDC)
