#  __  __ _____ _____   _    _           _            _       _
# |  \/  |  __ \_   _| | |  | |         | |          | |     (_)
# | \  / | |__) || |   | |__| | __ _ ___| |__        | | ___  _ _ __
# | |\/| |  ___/ | |   |  __  |/ _` / __| '_ \   _   | |/ _ \| | '_ \
# | |  | | |    _| |_  | |  | | (_| \__ \ | | | | |__| | (_) | | | | |
# |_|  |_|_|   |_____| |_|  |_|\__,_|___/_| |_|  \____/ \___/|_|_| |_|
# The MPI hash join algorithm with timing for benchmarking
# This algorithm makes use of the mpi4py library to implement a distributed MPI join algorithm.
# This File makes use of the collective communication paradigm through the use of the scatter, gather, broadcast and barrier commands, found withing mpi
# Input execution command: python MPI_Clean.py <table1FileName> <x> <table1FileName> <x> <tableoutFileName> <BenchmarkFileName>
# Please ensure that the table1, table2 and tableout file name are in JSON format
# Please specify the name of the BenchmarkFileName and ensure that it is a .txt file
# Where x and y are the column indices to join on.


from __future__ import unicode_literals
import json
from mpi4py import MPI
from collections import defaultdict
from prettytable import PrettyTable
import time
import sys

# MPI setup
comm = MPI.COMM_WORLD
size = comm.Get_size()
workers = size - 1
rank = comm.Get_rank()
name = MPI.Get_processor_name()
root = 0


# Define global timing variables
startTimeHash = time.time()
endTimeHash = time.time()
startTimeJoin = time.time()
endTimeJoin = time.time()
startTimeHashJoinFunction = time.time()
endTimeJoinHashJoinFunction = time.time()
startTimeReadFile1 = time.time()
endTimeJoinReadFile1 = time.time()
startTimeReadFile2 = time.time()
endTimeJoinReadFile2 = time.time()
startTimeScatter = time.time()
endTimeScatter = time.time()
startTimeBcast = time.time()
endTimeBcast = time.time()
startTimeBarrier = time.time()
endTimeBarrier = time.time()
startTimeGather = time.time()
endTimeGather = time.time()




def hashJoin(table1, index1, table2, index2):

    h = defaultdict(list)
    # hash phase

    startTimeHash = time.time()
    for s in table1:
        h[s[index1]].append(s)
    endTimeHash = time.time()
    # join phase
    startTimeJoin = time.time()
    finalSubJoin  = []
    for r in table2:
        for s in h[r[index2]]:
            finalSubJoin.append((s, r))
    endTimeJoin = time.time()

    return finalSubJoin

def printArray(array):
    for row in array:
        print(row)


def chunkify(lst,n):
    return [lst[i::n] for i in range(n)]


# function not used, because the
def distribute(length, num_nodes):
    div = length // num_nodes  # divide and floor
    rem = length % num_nodes
    i = 0
    node = 1
    output = []
    while i < length:
        if rem != 0:
            start = i
            i = (i + div) + 1
            end = i
            rem = rem - 1
        else:
            start = i
            i = i + div
            end = i
        output.append((start, end))
        node = node + 1
    return output

if len(sys.argv) != 7:
    if rank == 0:
        print("Please specify 4 parameters when running the program")

    sys.exit()

# identify which process is running what
sys.stdout.write(
    "Hello, World! I am process %d of %d on %s.\n"
    % (rank, size, name))

if rank == 0:
    startTimeFullRun = time.time()

    table1FileName = str(sys.argv[1])
    table2FileName = str(sys.argv[3])

    startTimeReadFile1 = time.time()
    table1 = []
    with open(table1FileName, 'r') as table1File:
        table1 = json.load(table1File)

    table2 = []
    with open(table2FileName, 'r') as table2File:
        table2 = json.load(table2File)
    endTimeJoinReadFile1 = time.time()
    elapsedTimeRead1 = str((endTimeJoinReadFile1 - startTimeReadFile1) * 1.0)


    if len(table1) > len(table2):
        smallerTable = 2
    else:
        smallerTable = 1

    chunkedTable = chunkify(eval("table" + str(smallerTable)), size)  #only chunk the smaller table

    completeTable = eval("table" + str(1+(smallerTable%2)))

if rank != 0:
    chunkedTable = None
    completeTable = None

startTimeScatter = time.time()
chunkedTable = comm.scatter(chunkedTable, root=0)
endTimeScatter = time.time()


# Broadcast the other table
startTimeBcast = time.time()
completeTable = comm.bcast(completeTable, root=0)
endTimeBcast = time.time()


startTime2 = time.time()

joinedResults = []

index1 = int(sys.argv[2])
index2 = int(sys.argv[4])


if len(chunkedTable) >= len(completeTable):
    endTimeJoinHashJoinFunction = time.time()
    joinedResults = hashJoin(completeTable,index1,chunkedTable,index2)
    endTimeJoinHashJoinFunction = time.time()
if len(chunkedTable) < len(completeTable):
    joinedResults = hashJoin(chunkedTable,index2,completeTable,index1)

startTimeBarrier = time.time()
# comm.Barrier()
endTimeBarrier = time.time()


startTimeGather = time.time()
finalJoin = comm.gather(joinedResults, root=0)
endTimeGather = time.time()
endTime2 = time.time()




if rank == 0:
    elapsedTimeFullRun = time.time() - startTimeFullRun
    outputFileName = sys.argv[5]
    flattenedJoin = [item for sublist in finalJoin for item in sublist]
    with open(outputFileName, 'w') as output_file:
        json.dump(flattenedJoin, output_file)





    # printArray(flattendJoin)

elapsedTimeHash = str((endTimeHash - startTimeHash)*1.0)
elapsedTimeJoin = str((endTimeJoin - startTimeJoin)*1.0)
elapsedTimeHashJoinFunction = str((endTimeJoinHashJoinFunction - startTimeHashJoinFunction)*1.0)
elapsedTimeScatter = str((endTimeScatter - startTimeScatter)*1.0)
elapsedTimeBcast = str((endTimeBcast - startTimeBcast)*1.0)
elapsedTimeBarrier = str((endTimeBarrier - startTimeBarrier)*1.0)
elapsedTimeGather = str((endTimeGather - startTimeGather)*1.0)
elapsedTimeSaviour = (endTime2 - startTime2)*1.0
# print("Process: %d, Hash time: %s, Join time: %s, HashJoin time: %s,  Scatter time: %s, Broadcast Time: %s, , Gather Time: %s"
#       % (rank, elapsedTimeHash, elapsedTimeJoin, elapsedTimeHashJoinFunction, elapsedTimeScatter, elapsedTimeBcast, elapsedTimeGather))


if rank == 0:
    x = PrettyTable()

    x.field_names = ["Benchmark", "Time (s)"]
    x.align["Benchmark"] = "l"
    x.add_row(["Processes", size])
    x.add_row(["Read Files", elapsedTimeRead1])
    x.add_row(["Hash Join", elapsedTimeHashJoinFunction])
    x.add_row(["Scatter", elapsedTimeScatter])
    x.add_row(["Broadcast", elapsedTimeBcast])
    x.add_row(["Barrier", elapsedTimeBarrier])
    x.add_row(["Gather", elapsedTimeGather])
    x.add_row(["Total", elapsedTimeFullRun])
    x.add_row(["Saviour", elapsedTimeSaviour])
    csv = [size,elapsedTimeFullRun,elapsedTimeSaviour]

    benchmarksFileName = sys.argv[6]
    with open(benchmarksFileName, 'a+') as benchmarkFile:
        benchmarkFile.write('***MPI Benchmark Results*** \n')
        benchmarkFile.write(str(x))
        benchmarkFile.write('\n\n')
    benchmarkFile.close()

    with open(str(benchmarksFileName[:-3]+"csv"), 'a+') as benchmarkCSV:
        benchmarkCSV.write(str(csv)[1:-1])
        benchmarkCSV.write('\n')
    benchmarkCSV.close()

