import sys
import json
from mpi4py import MPI
from collections import defaultdict
import numpy as np

comm = MPI.COMM_WORLD
size = comm.Get_size()
workers = size - 1
rank = comm.Get_rank()


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

    if (len(table1) > len(table2)):
        smallerTable = 2
    else:
        smallerTable = 1

    numberOfRowsSmallerTable = len(eval("table" + str(smallerTable)))
    workerIndecies = distribute(numberOfRowsSmallerTable, workers)

    print("Workers: %d\nNumber of rows in smaller table: %d\nWorker indexes: %s" % (
        workers,
        numberOfRowsSmallerTable,
        workerIndecies))

    for index, row in enumerate(workerIndecies):
        print("worker: %s operates on row %d to %d"%(index+1, row[0],row[1]))
        comm.send(table)

    # comm.send(table1, dest=1)
    # comm.send(table2, dest=1)

#     result = comm.recv()
#     print("result:")
#     printArray(result)
#
# elif rank == 1:
#
#     table1 = comm.recv()
#     print ("rank %d: %s" % (rank, table1))
#
#     table2 = comm.recv()
#     print ("rank %d: %s" % (rank, table2))
#
#     output = []
#     for row in hashJoin(table1, 0, table2, 0):
#         # print(row)
#         output.append(row)
#     comm.send(output,dest=0)