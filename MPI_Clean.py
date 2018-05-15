#  __  __ _____ _____   _    _           _            _       _
# |  \/  |  __ \_   _| | |  | |         | |          | |     (_)
# | \  / | |__) || |   | |__| | __ _ ___| |__        | | ___  _ _ __
# | |\/| |  ___/ | |   |  __  |/ _` / __| '_ \   _   | |/ _ \| | '_ \
# | |  | | |    _| |_  | |  | | (_| \__ \ | | | | |__| | (_) | | | | |
# |_|  |_|_|   |_____| |_|  |_|\__,_|___/_| |_|  \____/ \___/|_|_| |_|
# The MPI hash join algorithm without timing for clarity
# This algorithm makes use of the mpi4py library to implement a distributed MPI join algorithm.
# This File makes use of the  collective communication paradigm through the use of the scatter, gather, broadcast and barrier commands, found withing mpi
# Input execution command: python MPI_Clean.py <table1FileName> <x> <table1FileName> <x> <tableoutFileName> <BenchmarkFileName>
# Please ensure that the table1, table2 and tableout file name are in JSON format
# Please specify the name of the BenchmarkFileName and ensure that it is a .txt file
# Where x and y are the column indices to join on.

from __future__ import unicode_literals
import json
from mpi4py import MPI
from collections import defaultdict
import time
import sys

# MPI setup
comm = MPI.COMM_WORLD
size = comm.Get_size()
workers = size - 1
rank = comm.Get_rank()
name = MPI.Get_processor_name()
root = 0


def hashJoin(table1, index1, table2, index2):
    # defaultdict in python is in the form of a hash table
    h = defaultdict(list)
    # hash phase
    # append values into hash table
    for s in table1:
        h[s[index1]].append(s)
    # join phase
    # append matching rows bases on the key
    finalSubJoin = []
    for r in table2:
        for s in h[r[index2]]:
            finalSubJoin.append((s, r))
    return finalSubJoin


# function to split table into smaller segments according to number of processes
def chunkify(table, n):
    return [table[i::n] for i in range(n)]


# function orginaly used to generate indeices to split table when using MPI send and recv, not used anympore
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
    # assign file name according to runtime parameters
    table1FileName = str(sys.argv[1])
    table2FileName = str(sys.argv[3])

    # load input data into table1 an d table2 variables
    table1 = []
    with open(table1FileName, 'r') as table1File:
        table1 = json.load(table1File)

    table2 = []
    with open(table2FileName, 'r') as table2File:
        table2 = json.load(table2File)

    # calculate which of the two tables is smaller to determine which one to send to split and send to other processes
    if len(table1) > len(table2):
        smallerTable = 2
    else:
        smallerTable = 1

    # only chunk the larger table to help with efficiency
    chunkedTable = chunkify(eval("table" + str(smallerTable)), size)
    # load the smaller into memory
    completeTable = eval("table" + str(1 + (smallerTable % 2)))

# assign the chunkedTable and complete table to None if they arent the root process
if rank != 0:
    chunkedTable = None
    completeTable = None

# MPI scatter function was used to split the table data and send portions according to the chunked tables to other processes
chunkedTable = comm.scatter(chunkedTable, root=0)
# MPI broadcast function to send the entire of the smaller table to all other processes
completeTable = comm.bcast(completeTable, root=0)

joinedResults = []
# fetch key to join on from the run time parameters
index1 = int(sys.argv[2])
index2 = int(sys.argv[4])

# call the hash join function and specify right parameters depending on the size of the tables and the key to join on
if len(chunkedTable) >= len(completeTable):
    joinedResults = hashJoin(completeTable, index1, chunkedTable, index2)
if len(chunkedTable) < len(completeTable):
    joinedResults = hashJoin(chunkedTable, index2, completeTable, index1)

# Barrier is used to block until all processes in the communicator have reached this routine.
comm.Barrier()
# Gather retrieves all the sub joined table from other processes onto a single process
finalJoin = comm.gather(joinedResults, root=0)

# output the entire joined final table to json file on the root process according to run time parameters
if rank == 0:
    outputFileName = sys.argv[5]
    flattenedJoin = [item for sublist in finalJoin for item in sublist]
    with open(outputFileName, 'w') as output_file:
        json.dump(flattenedJoin, output_file)
