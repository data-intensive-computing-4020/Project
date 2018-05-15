# ELEN4020 - Data Intensive Computing
## Project - Equi-join of Relational Databases
The generator takes in four run time parameters: the number of rows to generate in the output tables, the name of the two output tables and the generation mode. An example execution of this application is:
python generator.py 100 table1.json table2.json best

A sample execution of 100 rows, 10 nodes, executing MPI MapReduce and Naive, with best case data looks as follows:
python controller.py 100 10 MRN best

The result checker takes in 6 arguments, two per join algorithm output. Each pair per algorithm output corresponds to the table generated by the algorithm, followed by the column on which the join must be performed. An example of this execution is:
python resultCheck.py NaiveResults.json 0 MapReduceResults.json 0 MPIResults.json 0

For example, one could run tests on all three scripts with row counts of 100, 1000, 10000, 100000 with node count for mpi of 1,2,4,8,16, with the best case sample data with the following command. 
python benchmark.py "[100,1000,10000,100000]" "[1,2,4,8,16]" best MR


This will then run 20 benchmarks for MPI (4 different sample sizes, over 5 different node counts) and 4 tests for mapreduce (4 different sample sizes) and ensure that at each output, the results are correct. The benchmark results from each execution are saved to disk, showing key run time metric parameters. An example output is shown below for MPI running on a 100000 row table

***MPI Benchmark Results***




+------------+----------------+


| Benchmark  |    Time (s)    |


+------------+----------------+


| Processes  |       4        |


| Read Files | 0.316300868988 |


| Hash Join  |      0.0       |


| Scatter    | 0.263787984848 |


| Broadcast  | 0.32134103775  |


| Barrier    | 0.121190071106 |


| Gather     | 0.809396028519 |


| Total      | 3.44939398766  |


+------------+----------------+

