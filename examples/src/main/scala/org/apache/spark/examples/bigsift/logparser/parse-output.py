
import sys
import csv

times={}
listoutput=[]
temp_time=-1
temp_run=-1
temp_iter=-1
list_size=[]
with open(sys.argv[1]) as file:
    print sys.argv[1] + "\n"
    i = 0
    for line in file:
	line = line.replace(":" , "")
        if("Runs" in line ):
#	    print line
	    sline = line.split()
	    temp_run = sline[5]
	if("Size" in line ):
            sline = line.split()
	    if(sline[5] not in list_size):
	    	listoutput.append([temp_run, temp_time , sline[5]])
	    	list_size.append(sline[5])
	if("Time" in line):
	    sline = line.split()
            temp_time = sline[5]
	if("iteration" in line and "partitions" not in line):
	    for key in listoutput:
		print str(key[0]) + ", " + str(key[1]) + "," + str(key[2])
	    print "\n\n\n\n" + line
	    listoutput = []
            list_size = []

	    

for key in listoutput:
    print str(key[0]) + ", " + str(key[1]) + "," + str(key[2])

