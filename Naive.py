import re
import os
import sys
import time
import json
from prettytable import PrettyTable

def Naive(file1_name, index_1, file2_name, index_2, output_file_name, benchmark_file_name):

    start_time = time.time()
    fileread_starttime = time.time()
    file_1 = open(file1_name, 'r')
    table_1 = file_1.read()
    file_2 = open(file2_name, 'r')
    table_2 = file_2.read()

    fileread_endtime = time.time()


    #Load the tables into JSON format
    table_1 = json.loads(table_1)
    table_2 = json.loads(table_2)
    joined_table = []

    #For each tuple of each table, check that the keys match
    join_starttime = time.time()
    for values_1 in table_1:
        for values_2 in table_2:
            if values_1[index_1] == values_2[index_2]:
                value = (values_1, values_2)
                joined_table.append(value)
    join_endtime = time.time()
    #Write the output to JSON file

    write_starttime = time.time()
    with open(output_file_name, 'w') as output_file:
        json.dump(joined_table, output_file)
    write_endtime = time.time()

    finish_time = time.time()
    total_time = finish_time - start_time
    fileread_time = fileread_endtime - fileread_starttime
    join_time = join_endtime - join_starttime
    write_time = write_endtime - write_starttime

    benchmark_table = PrettyTable()
    benchmark_table.field_names = ["Benchmark", "Time (s)"]
    benchmark_table.align["Benchmark"] = "l"
    benchmark_table.add_row(["Read both input files time: ", fileread_time])
    benchmark_table.add_row(["Join time: ", join_time])
    benchmark_table.add_row(["Write output file time: ", write_time])
    benchmark_table.add_row(["Total time: ", total_time])
    
    with open(benchmark_file_name, 'a+') as benchmark_file:
        benchmark_file.write('\t***Naive Benchmark Results*** \n')
        benchmark_file.write(str(benchmark_table))
        benchmark_file.write('\n \n')
    benchmark_file.close()

command_line = sys.argv
file1_name = command_line[1]
index_1 = int(command_line[2])
file2_name = command_line[3]
index_2 = int(command_line[4])
output_file_name = command_line[5]
benchmark_file_name = command_line[6]

Naive(file1_name,index_1, file2_name, index_2, output_file_name, benchmark_file_name)