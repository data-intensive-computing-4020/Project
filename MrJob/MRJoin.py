from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os
import sys
import time	
import json	
import hashlib		

class MRJoin(MRJob):
		count = 0;
		def steps(self):
			return [MRStep(mapper = self.mapper_1, reducer = self.reducer_1), MRStep(reducer = self.reducer_2)]

		def mapper_1(self, _, line):
		
			filename = os.environ['map_input_file']
			data = json.loads(line)
			
			for x in range(0, len(data)):
				value = data[x]
				hash_key = hashlib.md5(value[0]).hexdigest()
				
				if filename == sys.argv[1]:
					yield hash_key, (value, 'A')
				else:
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

			#yield _, values
			output = []
			for val in values:
				output.append(val)
				
			print(output)

MRJoin.run()
