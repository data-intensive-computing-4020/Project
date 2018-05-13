import sys
import os

rowSteps = eval(sys.argv[1])
nodeSteps = eval(sys.argv[2])

for row in rowSteps:
    for node in nodeSteps:
        print("row: {} node: {}".format(row,node))
        os.system("python controller.py {} {} M".format(row,node))