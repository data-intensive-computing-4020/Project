from mpi4py import MPI

soft=MPI.INFO_ENV.get("soft")
print soft
maxprocs=MPI.INFO_ENV.get("maxprocs")
print maxprocs
