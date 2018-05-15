# Project
The generator takes in four run time parameters: the number of rows to generate in the output tables, the name of the two output tables and the generation mode. An example execution of this application is:
python generator.py 100 table1.json table2.json best

A sample execution of 100 rows, 10 nodes, executing MPI MapReduce and Naive, with best case data looks as follows:
python controller.py 100 10 MRN best

The result checker takes in 6 arguments, two per join algorithm output. Each pair per algorithm output corresponds to the table generated by the algorithm, followed by the column on which the join must be performed. An example of this execution is:
python resultCheck.py NaiveResults.json 0 MapReduceResults.json 0 MPIResults.json 0
