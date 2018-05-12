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
			return [MRStep(mapper = self.mapper_1, reducer = self.reducer_1)]

		def mapper_1(self, _, line):
		
			filename = os.environ['map_input_file']
			data = json.loads(line)
			
			for x in range(0, len(data)):
				value = data[x]
				hash_key = hashlib.md5(value[0]).hexdigest()
				
				if filename == sys.argv[-2]:
					yield hash_key, value
				else:
					yield hash_key, value		
				
		def reducer_1(self, hash_key, values):
			output = []
			values_list = list(values)
			num_values = len(values_list)
			if num_values == 2:
				for num_val in values_list:
					for i in num_val:
						output.append(i)
				yield None, output	
				
			elif num_values%2 == 0:
				print(values_list)
				A = values_list[:len(values_list)/2]
				B = values_list[len(values_list)/2:]
				
				for y in range(0,len(A)):
					for x in range(0,len(B)):
						temp = A[y] + B[x]
						yield None, temp
			
MRJoin.run()
