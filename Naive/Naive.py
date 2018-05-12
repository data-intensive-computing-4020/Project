import re
import os
import sys
import time
import json

# def Main(filename1, filename2):
  
file_1 = open('table1.json', 'r')
table_1 = file_1.read()
file_2 = open('table2.json', 'r')
table_2 = file_2.read()


table_1 = json.loads(table_1)
table_2 = json.loads(table_2)
joined_table = []

for values_1 in table_1:
    for values_2 in table_2:
        if values_1[0] == values_2[0]:
            value = values_1 + values_2
            joined_table.append(value)

#joined_table = json.dumps(joined_table)

with open('output.json', 'w') as output_file:
    json.dump(joined_table, output_file)

#print(joined_table)
