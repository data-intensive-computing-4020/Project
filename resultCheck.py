# Very basic application to check if the three algorithm outputs yield the same join results

import sys
import json
from operator import itemgetter, attrgetter, methodcaller
from colours import bcolors

table1 = []
table2 = []
table3 = []

print(bcolors.BOLD+bcolors.HEADER+"Checking results..."+bcolors.ENDC)

if (len(sys.argv) == 1):
    print(bcolors.FAIL+"No arguments provided! Input the json table name followed by the location of the key on which the join occured to preform a result check"+bcolors.ENDC)
    sys.exit()
if ((len(sys.argv)-1)%2!=0):
    print(bcolors.FAIL+"Invalid input format! Each table requires an associated index corresponding to the key location where the join occured"+bcolors.ENDC)
    sys.exit()
print(bcolors.OKBLUE+"Opening file {} with index {} for table1".format(sys.argv[1],sys.argv[2])+bcolors.ENDC)
with open(sys.argv[1], 'r') as table1File:
    table1 = json.load(table1File)
index1=sys.argv[2]

print(bcolors.OKBLUE+"Opening file {} with index {} for table2".format(sys.argv[3],sys.argv[4])+bcolors.ENDC)
with open(sys.argv[3], 'r') as table1File:
    table2 = json.load(table1File)
    index2=sys.argv[4]

print(bcolors.OKBLUE+"Opening file {} with index {} for table3".format(sys.argv[5],sys.argv[6])+bcolors.ENDC)
with open(sys.argv[5], 'r') as table1File:
    table3 = json.load(table1File)
index3=sys.argv[6]

print(bcolors.OKBLUE+"3 tables loaded for checking consistency"+bcolors.ENDC)

table1 = sorted(table1,key=itemgetter(0))
table2 = sorted(table2,key=itemgetter(0))
table3 = sorted(table3,key=itemgetter(0))

print(bcolors.OKBLUE+"3 tables sorted"+bcolors.ENDC)

if(table1==table2 and table1==table3 and table2 == table3):
    print(bcolors.OKGREEN+bcolors.BOLD+"Tables match"+bcolors.ENDC)

else:
    print(bcolors.FAIL + bcolors.BOLD + "Tables did not match!" + bcolors.ENDC)
    if (table1 != table2):
        print(bcolors.WARNING+"table1 != table2"+bcolors.ENDC)
    if (table1 != table3):
        print(bcolors.WARNING+"table1 != table3"+bcolors.ENDC)
    if (table2 != table3):
        print(bcolors.WARNING+"table2 != table3"+bcolors.ENDC)
