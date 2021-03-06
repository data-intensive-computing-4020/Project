# ELEN4020 - Data Intensive Computing
## Project - Equi-join of Relational Databases
Some prior input data was generated and is ocntained in the repo. This allows for one to run the the individual algorithms without the utility scripts. 
### MPI Hash Join 
This algorithm makes use of the mpi4py library to implement a distributed MPI join algorithm. Please see MPIJoin_Clean.py for a clear code base without timing to see how the code works.
```bash
# To run the Collective communication MPI implemention:
python MPIJoin.py table1.json 0 table2.json 0 output.json benchmark.txt

# To run the Collective communication MPI implemention:
python MPIJoin_Send_Recive.py  table1.json 0 table2.json 0 output.json benchmark.txt
```
### MapReduce Reduce-Side Join
This algorithm utilizes the [MrJob](https://github.com/Yelp/mrjob) library to implement MapReduce funtionality for the joining of 2 tables on a common column. The reduce-side join is selected and it performs the join during the reducer phase of the job. 

```bash
# To run the script
python MRJoin.py table1.json 0 table2.json 0 output.json benchmark.txt
```

### Naive Join
The Naive algorithm that utilizes two nested for loops.

```bash
# To run the script
python Naive.py table1.json 0 table2.json 0 output.json benchmark.txt
```

## Utility Scripts
### Benchmark Script
This script calls both the generator and the controller scripts in order to automate and make the testing process simple. This file only works in the context of running on the jaguar cluster as the machinefile is configured for the cluster. For example, one could run tests on all three scripts with row counts of 100, 1000, 10000, 100000 with node count for mpi of 1,2,4,8,16, with the best case sample data with the following command. 

```bash
# To run the script
python benchmark.py "[100,1000,10000,100000]" "[1,2,4,8,16]" best MR
```
### Input Data Generator Script

The generator takes in four run time parameters: the number of rows to generate in the output tables, the name of the two output tables and the generation mode. An example execution of this application is:

```bash
# To run the script
python generator.py 100 table1.json table2.json best
```
A sample execution of 100 rows, 10 nodes, executing MPI MapReduce and Naive, with best case data looks as follows:
python controller.py 100 10 MRN best


### Result Checker

The result checker takes in 6 arguments, two per join algorithm output. Each pair per algorithm output corresponds to the table generated by the algorithm, followed by the column on which the join must be performed. An example of this execution is:
```bash
# To run the script
python resultCheck.py NaiveResults.json 0 MapReduceResults.json 0 MPIResults.json 0
```
