#   __  __             _____          _                  _____          _                       _____ _     _          _       _
# |  \/  |           |  __ \        | |                |  __ \        | |                     / ____(_)   | |        (_)     (_)
# | \  / | __ _ _ __ | |__) |___  __| |_   _  ___ ___  | |__) |___  __| |_   _  ___ ___ _____| (___  _  __| | ___     _  ___  _ _ __
# | |\/| |/ _` | '_ \|  _  // _ \/ _` | | | |/ __/ _ \ |  _  // _ \/ _` | | | |/ __/ _ \______\___ \| |/ _` |/ _ \   | |/ _ \| | '_ \
# | |  | | (_| | |_) | | \ \  __/ (_| | |_| | (_|  __/ | | \ \  __/ (_| | |_| | (_|  __/      ____) | | (_| |  __/   | | (_) | | | | |
# |_|  |_|\__,_| .__/|_|  \_\___|\__,_|\__,_|\___\___| |_|  \_\___|\__,_|\__,_|\___\___|     |_____/|_|\__,_|\___|   | |\___/|_|_| |_|
#              | |                                                                                                  _/ |
#              |_|                                                                                                 |__/
# The MapReduce reduce-side join algorithm
# Input execution command: python MPI_Join.py <table1FileName> <x> <table1FileName> <x> <tableoutFileName> <BenchmarkFileName>
# Please ensure that the table1, table2 and tableout file name are in JSON format
# Please specify the name of the BenchmarkFileName and ensure that it is a .txt file
# Where x and y are the column indices to join on.

from mrjob.job import MRJob
from mrjob.step import MRStep
from prettytable import PrettyTable
import re
import os
import sys
import time
import json

class MRJoin(MRJob):

    # This defines the steps the job will follow 
    def steps(self):
        return [MRStep(mapper=self.map_keys, reducer=self.reduce_keys), MRStep(reducer=self.reduce_for_output)]

    # This is the mapper phase that produces the key-value pairs
    def map_keys(self, _, line):
        filename = os.environ['map_input_file']
        table = json.loads(line)

        for record in table:
            if filename == sys.argv[1]:
                key = str(record[join_field_1])
                yield key, (record, 'A')
            else:
                key = str(record[join_field_2])
                yield key, (record, 'B')

    # This is the reducer phase that joins the records from the table
    def reduce_keys(self, key, records):
        output = []
        records_list = list(records)
        num_records = len(records_list)
        # Simple join of matching records
        if num_records == 2:
            for num_val in records_list:
                for i in num_val:
                    if i != 'A' and i != 'B':
                        output.append(i)
            output = (output[0], output[1])
            yield None, output
				
	# Join of records when there are multiple values in both tables corresponding to the same key
        elif num_records % 2 == 0:
            A = records_list[:len(records_list) / 2]
            B = records_list[len(records_list) / 2:]
            for y in A:
                for x in B:
                    joined_value = (y[0], x[0])
                    yield None, joined_value
				
        # Join of records when there is one value in one table and multiple values in another correspondning to the same key
        elif num_records % 2 == 1:
            count = 0;
            A = []
            B = []
            for j in records_list:
                if j[1] == 'A':
                    A.append(j[0])
                else:
                    B.append(j[0])
            for y in A:
                for x in B:
                    joined_value = (A, B)
                    yield None, joined_value

    # This is a reducer phase for displaying all the records in the resultant table in a clear json format		
    def reduce_for_output(self, _, records):
        output = []
        for val in records:
            output.append(val)

        global final_output
        final_output = output

# Extraction of arguments from the command line
command_line = sys.argv

join_field_1 = int(command_line[2])
join_field_2 = int(command_line[4])
output_file_name = command_line[5]
benchmark_file_name = command_line[6]

sys.argv = [command_line[0], command_line[1], command_line[3]]

# Creation of the output benchmark table for the timing results
final_output = []
time_table = PrettyTable()
time_table.field_names = ["Benchmark", "Time taken"]

# Timing of the algorithm
start_time = time.time()
MRJoin.run()
end_time = time.time()
total_time = end_time - start_time

# Addition of the timed result to the output benchmark table
time_table.add_row(["Total time", str(total_time)])

# Writing an output text file
time_write = open(benchmark_file_name, 'a+')
time_write.write("***MRJoin Benchmark Results*** \n")
time_write.write(str(time_table))
time_write.write("\n\n")
time_write.close()

with open(output_file_name, 'w') as output_file:
    json.dump(final_output, output_file)
