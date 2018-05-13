from __future__ import unicode_literals
import json
from mpi4py import MPI
from collections import defaultdict
import time

# MPI setup
comm = MPI.COMM_WORLD
size = comm.Get_size()
workers = size - 1
rank = comm.Get_rank()
root = 0


def hashJoin(table1, index1, table2, index2):
    h = defaultdict(list)
    # hash phase
    for s in table1:
        h[s[index1]].append(s)
    # join phase
    return [(s, r) for r in table2 for s in h[r[index2]]]


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


if rank == 0:

    table1 = []
    with open("table1.json", 'r') as table1File:
        table1 = json.load(table1File)

    table2 = []
    with open("table2.json", 'r') as table2File:
        table2 = json.load(table2File)

    # Start the timer
    startTime = time.time()

    if (len(table1) > len(table2)):
        smallerTable = 2
    else:
        smallerTable = 1

    chunkedTable = chunkify(eval("table" + str(smallerTable)), size)  #only chunk the smaller table

    completeTable = eval("table" + str(1+(smallerTable%2)))

if rank != 0:
    chunkedTable = None
    completeTable = None

chunkedTable = comm.scatter(chunkedTable, root=0)

# Broadcast the other table

completeTable = comm.bcast(completeTable, root=0)


joinedResults = []

if len(chunkedTable) >= len(completeTable):
    joinedResults = hashJoin(completeTable,0,chunkedTable,0)

if len(chunkedTable) < len(completeTable):
    joinedResults = hashJoin(chunkedTable,0,completeTable,0)

comm.Barrier()

finalJoin = comm.gather(joinedResults, root=0)


if rank == 0:
    flattendJoin = [item for sublist in finalJoin for item in sublist]
    elapsedTime = time.time() - startTime
    print("Nodes: %d \nTime: %s s" % (size, str(elapsedTime * 1.0)))

    # printArray(flattendJoin)

