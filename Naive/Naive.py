import re
import os
import sys
import time
import json


def Naive(file1_name, index_1, file2_name, index_2, output_file_name):

    start_time = time.time()
    file_1 = open(file1_name, 'r')
    table_1 = file_1.read()
    file_2 = open(file2_name, 'r')
    table_2 = file_2.read()

    table_1 = json.loads(table_1)
    table_2 = json.loads(table_2)
    joined_table = []

    for values_1 in table_1:
        for values_2 in table_2:
            if values_1[index_1] == values_2[index_2]:
                value = (values_1, values_2)
                joined_table.append(value)

    with open(output_file_name, 'w') as output_file:
        json.dump(joined_table, output_file)

    finish_time = time.time()
    total_time = finish_time - start_time
    print(total_time)

command_line = sys.argv
file1_name = command_line[1]
index_1 = int(command_line[2])
file2_name = command_line[3]
index_2 = int(command_line[4])
output_file_name = command_line[5]

Naive(file1_name,index_1, file2_name, index_2, output_file_name)