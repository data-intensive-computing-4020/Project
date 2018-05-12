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
			return [MRStep(mapper = self.mapper_1, reducer = self.reducer_1)]

		def mapper_1(self, _, line):
		
			filename = os.environ['map_input_file']
			data = json.loads(line)
			
			for x in range(0, len(data)):
				value = data[x]
				hash_key = hashlib.md5(value[0]).hexdigest()
				
				if filename == sys.argv[-2]:
					yield (hash_key,x), value
				else:
					yield (hash_key,x), value[1:]		
				
		def reducer_1(self, hash_key, value):
			
			output = []
			for val in value:
				for x in val:
					output.append(x)
			yield hash_key[1],output
   
MRJoin.run()
