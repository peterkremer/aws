# Requires pymongo
import pymongo
#import mongohq

import csv
import sys
from itertools import islice
from pymongo import MongoClient


########## MongoHQ databases ##############
# Need to modify this so that the user and password are stored separately
# ideagensURI = "mongodb://experimenter:1#dJ3VYSf8Sn5iE9@kahana.mongohq.com:10075/"
# ideagensDBName = 'IdeaGens'
# ideagenstestURI = "mongodb://sandbox:protolab1@kahana.mongohq.com:10056/"
# ideagenstestDBName = 'IdeaGensTest'

client = MongoClient(sys.argv[1])
db = client['meteor']
ideasToProc = db.ideasToProcess

with open("./data/Mike_Terry_instance.csv", "rU") as csvfile:
	datareader = csv.reader(csvfile, delimiter=",")
	for row in datareader:
		data = row
		if(data[3] == 'iPod'): #just get answers to iPod prompt
			idea = {'content': data[8], 'participantID': data[1]}
			idea_id = ideasToProc.insert(idea)
			#print "_id: %s, participantID: %s, content: %s" % (idea_id, idea['participantID'], idea['content']) 


