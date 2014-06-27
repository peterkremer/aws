# Requires pymongo
import pymongo
#import mongohq

import csv
import sys
from itertools import islice
from pymongo import MongoClient


########## MongoHQ databases ##############
# Need to modify this so that the user and password are stored separately
# ideagensURI = "mongodb://experimenter:@kahana.mongohq.com:10075/"
# ideagensDBName = 'IdeaGens'
# ideagenstestURI = "mongodb://sandbox:protolab1@kahana.mongohq.com:10056/"
# ideagenstestDBName = 'IdeaGensTest'

client = MongoClient(sys.argv[1])
db = client['meteor']
ideasToProc = db.ideasToProcess
ideas = db.ideas
clusters = db.clusters

def upload_ideasToProc():
	with open("./data/Mike_Terry_instance.csv", "rU") as csvfile:
		datareader = csv.reader(csvfile, delimiter=",")
		for data_row in datareader:
			if(data_row[3] == 'iPod'): #just get answers to iPod prompt
				idea_instance = {'_id': data_row[17], 'content': data_row[8], 'participantID': data_row[1] 'inCluster': 'false'}
				ideasToProc.insert(idea_instance)
				#print "_id: %s, participantID: %s, content: %s" % (idea_id, idea['participantID'], idea['content'])

def upload_tree():
	with open("./data/trees_withNames.csv", "rU") as csvfile:
		datareader = csv.reader(csvfile, delimiter=",")
		for data_row in datareader:
			if(data_row[16] == 'iPod'):
				clus_label = data_row[4] #plain text label for the idea node
				clus_rawtext = data_row[5]
				clus_id = data_row[3]
				#Subtrees are all children of root, which has _id: "-1". Clusters collection has predefined root cluster with matching _id
				parent_id = data_row[15]

				clus_name = ""#label if provided, rawtext if no label, id if no rawtext
				if (clus_label != ""):
					clus_name = clus_label
				elif (clus_rawtext != ""):
					clus_name = clus_rawtext
				else:
					clus_name = clus_id

				#create cluster and add to cluster collection
				cluster = {'_id': clus_id, 'name': clus_name, 'ideas': [], 'children': []}
				clusters.insert(cluster)
				#get parent and add cluster to its children list
				clusters.update({'_id': parent_id}, {"$push": {'children': clus_id}}, upsert=False)
	link_instances()

def link_instances():
	with open("./data/Mike_Terry_instance.csv", "rU") as csvfile:
		datareader = csv.reader(csvfile, delimiter=",")
		for data_row in datareader:
			if(data_row[3] == 'iPod'): #just get answers to iPod prompt
				idea_instance = {'_id': data_row[0], 'content': data_row[8], 'participantID': data_row[1], 'inCluster' : 'true'}
				ideas.insert(idea_instance)	
				parent_id = data_row[18] # which cluster(idea node) the instance belongs to
				clusters.update({'_id': parent_id}, {"$push": {'ideas': idea_instance}}, upsert=False)

upload_tree()



