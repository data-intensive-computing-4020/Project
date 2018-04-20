import sys, os, re
from mrjob.job import MRJob

class MRJoin(MRJob):
  
	SORT_VALUES = True
  
	def mapper(self, _, line):    

		values = line.rstrip("\n").split("|")
    
		if len(values) == 2:  
			countryName, country2digit = line.rstrip("\n").split("|")
			yield country2digit, (countryName)
		else:
			personName, personType, country2digit = line.rstrip("\n").split("|")
			yield country2digit, (personName, personType)
  
	def reducer(self, key, values):

		values = [x for x in values]
		if len(values) > 1: 
			country = values[0]
			for value in values[1:]:
				yield key, (country, value)
		else:
			pass
      
if __name__ == '__main__':
	MRJoin.run()
