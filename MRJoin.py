from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os
import sys
import time	
import json		

class MRJoin(MRJob):
		
		def steps(self):
			return [MRStep(mapper = self.map_keys, reducer = self.reduce_keys), MRStep(reducer = self.reduce_for_output)]

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
				
		def reduce_keys(self, key, records):
			output = []
			records_list = list(records)
			num_records = len(records_list)
			if num_records == 2:
				for num_val in records_list:
					for i in num_val:
						if i != 'A' and i != 'B':
							output.append(i)
				output = (output[0], output[1])
				yield None, output	
				
			elif num_records%2 == 0:
				A = records_list[:len(records_list)/2]
				B = records_list[len(records_list)/2:]
				for y in A:
					for x in B:
						joined_value = (y[0], x[0])
						yield None, joined_value

			elif num_records%2 == 1:
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

		def reduce_for_output(self, _, records):
			output = []
			for val in records:
				output.append(val)
				
			global final_output
			final_output = output

command_line = sys.argv

join_field_1 = int(command_line[2])
join_field_2 = int(command_line[4])
output_file_name = command_line[5]
benchmark_file_name = command_line[6]

#Laura's sys.argv
#sys.argv = [command_line[0], command_line[1], command_line[3], command_line[6]]
sys.argv = [command_line[0], command_line[1], command_line[3]]

final_output = []

start_time = time.time()
MRJoin.run()
end_time = time.time()
total_time = end_time-start_time

with open(benchmark_file_name, 'w') as time_file:
	json.dump(total_time, time_file)


with open(output_file_name, 'w') as output_file:
	json.dump(final_output, output_file)
