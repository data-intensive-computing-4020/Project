#!/usr/bin/env python3

import numpy as np
from mpi4py import MPI
import random

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
root = 0

local_array = [rank] * random.randint(2, 5)
print("rank: {}, local_array: {}".format(rank, local_array))

sendbuf = np.array(local_array)
print(sendbuf)


# Collect local array sizes using the high-level mpi4py gather
print("other...rank: {}, value {}".format(rank, comm.gather(len(sendbuf), root)))
sendcounts = np.array(comm.gather(len(sendbuf), root))

if rank == root:
    print("sendcounts: {}, total: {}".format(sendcounts, sum(sendcounts)))
    recvbuf = np.empty(sum(sendcounts), dtype=int)

else:
    recvbuf = None

comm.Gatherv(sendbuf=sendbuf, recvbuf=(recvbuf, sendcounts), root=root)
if rank == root:
    print("Gathered array: {}".format(recvbuf))