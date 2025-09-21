import derive
import sys
import re
import sqlite3
import updateDatabase
import utils

# Copyright: CSIRO 2024
# Author: Agastya Kapur: Agastya.Kapur@csiro.au, George Hobbs: George.Hobbs@csiro.au

# Read in arguments

infile = open(sys.argv[1], "r")
citation_id = sys.argv[2]
delim = sys.argv[3]
commit = sys.argv[4] 
database = sys.argv[5]

# Set commit value
if(commit == "1"):
    commit = True
else:
    commit = False

# Connect to database. Use this connection for the rest of the process
con = utils.connect_db(database)

for line in infile:
    if(re.search("GROUP",line)):
        splitted = line.split(delim)
        if(len(splitted)!=3):
            print("Input file format not correct")
            exit()
        linkedSetDesc = (splitted[1]).strip()
        linkedSetType = (splitted[2]).strip()
        linkedSet_id = None
        
        if(int(linkedSetType) == 1):
            print("Adding new row data set for 1 pulsar")
            if(linkedSetDesc == ""):
                linkedSetDesc = None
            updateDatabase.addRows(con,sys.argv[1],citation_id,linkedSetDesc=linkedSetDesc,delim=delim,commit=commit)
        elif(int(linkedSetType) == 2):
            print("Adding Par File")
            if(linkedSetDesc == ""):
                linkedSetDesc = "Par file input"
            print(linkedSetDesc)
            updateDatabase.addParFile(con,sys.argv[1],citation_id,linkedSetDesc=linkedSetDesc,commit=commit)
        elif(int(linkedSetType) == 3):
            print("Not implemented for anything")
            if(linkedSetDesc == ""):
                linkedSetDesc = "Not implemented"
            print(linkedSetDesc)
        elif(int(linkedSetType) == 4):
            # Used for tabular format data and glitches/geometric parameters with _LINKEDSET,1 global parameter
            print("Column Based Tabular Input")
            if(linkedSetDesc == ""):
                linkedSetDesc = "Tabular input"
            print(linkedSetDesc)
            updateDatabase.addTabular(con,sys.argv[1],citation_id,delim=delim,commit=commit)
        else:
            print("Group Type Unknown")
            print("---")
            exit()
    else:
        exit()

