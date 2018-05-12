import sys
import json

t1 = open("table1.txt", 'w')
t2 = open("table2.txt", 'w')

table1 = []
table2 = []


for index in range(0,int(sys.argv[1])):
    key = "table"+str(index)
    table1.append((key,1,2,3,4))
    table2.append((key,5,6,7,8))

with open('table1.json','w') as tableOut1:
    json.dump(table1,tableOut1)

with open('table2.json','w') as tableOut2:
    json.dump(table2,tableOut2)