# Basic utility to generate data sets for benchmaring join algorithms.
import sys
import json
from colours import bcolors

table1 = []
table2 = []

if (len(sys.argv) != 5):
    print(
        bcolors.FAIL + "Bad input params! Requires 3 inputs: the number of rows to generate and the ouput names of the two files to create" + bcolors.ENDC)
    sys.exit(1)

rows = sys.argv[1]
fileName1 = sys.argv[2]
fileName2 = sys.argv[3]
mode = sys.argv[4]

if "equal" in mode:
    for index in range(0, int(rows)):
        key1 = "table" + str(index)
        key2 = "table" + str(int(sys.argv[1]) - index)
        table1.append((key1, "1", "2", "3", "4"))
        table2.append((key2, "5", "6", "7", "8"))

    with open(fileName1, 'w') as tableOut1:
        json.dump(table1, tableOut1)

    with open(fileName2, 'w') as tableOut2:
        json.dump(table2, tableOut2)

    print(bcolors.OKGREEN + "Tables generated with {} rows".format(rows) + bcolors.ENDC)

if "best" in mode:
    for index in range(0, int(rows)):
        key1 = "table" + str(index)
        table1.append((key1, "1", "2", "3", "4"))
        if index%5==0:
            key2 = "table" + str(int(sys.argv[1]) - index)
            table2.append((key2, "5", "6", "7", "8"))

    with open(fileName1, 'w') as tableOut1:
        json.dump(table1, tableOut1)

    with open(fileName2, 'w') as tableOut2:
        json.dump(table2, tableOut2)

    print(bcolors.OKGREEN + "Tables generated with {} rows".format(rows) + bcolors.ENDC)