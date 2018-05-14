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
startTimeSend = time.time()
endTimeSend = time.time()
startTimeRecv = time.time()
endTimeRecv = time.time()



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


if rank == 0:
    # Start the timer
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

    smallerTableLength = len(eval("table" + str(smallerTable)))
    biggerTableLength = len(eval("table" + str(1+(smallerTable%2))))
    chunkIndecies = distribute(biggerTableLength,size)
    biggerTableName = str("table" + str(1+(smallerTable%2)))
    smallerTableName = str("table" + str(smallerTable))

    chunkedTable = eval(biggerTableName)[chunkIndecies[0][0]:chunkIndecies[0][1]]
    completeTable = eval(smallerTableName)

    startTimeSend = time.time()
    for index, chunk in enumerate(chunkIndecies):
        if index == 0:
            continue
        req = comm.isend(eval(biggerTableName)[chunk[0]:chunk[1]],dest=index,tag=0)
        req.wait()
        req1 = comm.isend(eval(smallerTableName),dest=index,tag=1)
        req1.wait()
    endTimeSend = time.time()


if rank != 0:

    req = comm.irecv(source=0,tag=0)
    chunkedTable = req.wait()

    req1 = comm.irecv(source=0,tag=1)
    completeTable = req1.wait()


joinedResults = []

index1 = int(sys.argv[2])
index2 = int(sys.argv[4])


if len(chunkedTable) >= len(completeTable):
    endTimeJoinHashJoinFunction = time.time()
    joinedResults = hashJoin(completeTable,index1,chunkedTable,index2)
    endTimeJoinHashJoinFunction = time.time()
if len(chunkedTable) < len(completeTable):
    joinedResults = hashJoin(chunkedTable,index2,completeTable,index1)

if rank !=0:
    req = comm.isend(joinedResults,dest=0,tag=rank)
    req.wait()

if rank == 0:
    finalJoin = []
    finalJoin.append(joinedResults)
    for index in range(1,size):
        req = comm.irecv(source=index,tag=index)
        data = req.wait()
        finalJoin.append(data)

    flattenedJoin = [item for sublist in finalJoin for item in sublist]
    outputFileName = sys.argv[5]
    flattenedJoin = [item for sublist in finalJoin for item in sublist]
    with open(outputFileName, 'w') as output_file:
        json.dump(flattenedJoin, output_file)

    elapsedTimeFullRun = time.time() - startTimeFullRun
    elapsedTimeSend = endTimeSend - startTimeSend

    x = PrettyTable()

    x.field_names = ["Benchmark", "Time (s)"]
    x.align["Benchmark"] = "l"
    x.add_row(["Processes", size])
    x.add_row(["Read Files", elapsedTimeRead1])
    x.add_row(["Send", elapsedTimeSend])

    x.add_row(["Total", elapsedTimeFullRun])

    benchmarksFileName = sys.argv[6]
    with open(benchmarksFileName, 'a+') as benchmarkFile:
        benchmarkFile.write('***MPI Benchmark Results*** \n')
        benchmarkFile.write(str(x))
        benchmarkFile.write('\n\n')
    benchmarkFile.close()
