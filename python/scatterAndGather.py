import json
from mpi4py import MPI
from collections import defaultdict
import numpy as np

comm = MPI.COMM_WORLD
size = comm.Get_size()
workers = size - 1
rank = comm.Get_rank()


def chunkify(lst,n):
    return [lst[i::n] for i in range(n)]

if rank == 0:
    table = [["table0", 1, 2, 3, 4], ["table1", 1, 2, 3, 4], ["table2", 1, 2, 3, 4], ["table3", 1, 2, 3, 4],
             ["table4", 1, 2, 3, 4], ["table5", 1, 2, 3, 4], ["table6", 1, 2, 3, 4], ["table7", 1, 2, 3, 4]]

    data = chunkify(table,size)

if rank != 0:
    data = None

data = comm.scatter(data, root=0)

print("Rank %s, data: %s\n" % (rank, data))
