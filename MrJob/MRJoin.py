from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os
import sys
import time	
import json	
import hashlib		



class MRJoin(MRJob):
		
		def steps(self):
			return [MRStep(mapper = self.mapper_1, reducer = self.reducer_1), MRStep(reducer = self.reducer_2)]

		def mapper_1(self, _, line):
		
			filename = os.environ['map_input_file']
			data = json.loads(line)
			
			for x in range(0, len(data)):
				value = data[x]
				
				if filename == sys.argv[1]:
					hash_key = hashlib.md5(str(value[join_field_1])).hexdigest()
					yield hash_key, (value, 'A')
				else:
					hash_key = hashlib.md5(str(value[join_field_2])).hexdigest()
					yield hash_key, (value, 'B')		
				
		def reducer_1(self, hash_key, values):
			output = []
			values_list = list(values)
			num_values = len(values_list)
			if num_values == 2:
				for num_val in values_list:
					for i in num_val:
						if i != 'A' and i != 'B':
							output.append(i)
				output = (output[0], output[1])
				yield None, output	
				
			elif num_values%2 == 0:
				A = values_list[:len(values_list)/2]
				B = values_list[len(values_list)/2:]
				for y in range(0,len(A)):
					for x in range(0,len(B)):
						temp = (A[y][0], B[x][0])
						yield None, temp

			elif num_values%2 == 1:
				count = 0;
				A = []
				B = []
				for j in values_list:
					if j[1] == 'A':
						A.append(j[0])
					else:
						B.append(j[0])
				for y in range(0,len(A)):
					for x in range(0,len(B)):
						temp = (A[y], B[x])
						yield None, temp

		def reducer_2(self, _, values):

			output = []
			for val in values:
				output.append(val)
				
			#print(output)
			global final_output
			final_output = output

command_line = sys.argv
print(command_line)

join_field_1 = int(command_line[2])
join_field_2 = int(command_line[4])
output_file_name = command_line[5]
#Laura's sys.argv
#sys.argv = [command_line[0], command_line[1], command_line[3], command_line[6]]
sys.argv = [command_line[0], command_line[1], command_line[3]]

#python MRJoin.py table1.json 1 table2.json 1 output.json
final_output = []
MRJoin.run()

with open(output_file_name, 'w') as output_file:
	json.dump(final_output, output_file)