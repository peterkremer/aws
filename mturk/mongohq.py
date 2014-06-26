#!/usr/bin/env python

import sys
# Requires pymongo
import pymongo
from datetime import datetime
from pymongo import MongoClient


########## MongoHQ databases ##############
# Need to modify this so that the user and password are stored separately
ideagenstest = {'url': "kahana.mongohq.com",
                'port': 10056,
                'dbName': 'IdeaGensTest',
                'user': 'sandbox',
                'pswd': 'protolab1' }
# user and paswd are incorrect (do not want to commit secure info
ideagens = {'url': "kahana.mongohq.com",
            'port': 10075,
            'dbName': 'IdeaGens',
            'user': 'experimenter',
            'pswd':  sys.argv[1]}
#
ideagens_sandbox = {'url': "kahana.mongohq.com",
                    'port': 10068,
                    'dbName': 'ideagens_sandbox',
                    'user': 'peter',
                    'pswd': 'proto' }


def get_mongodb(dbUrl, dbPort, dbName, dbUser, dbPswd):
  """
  takes db parameters and returns a connected db object usign those
  parameters

  """
  dbURI = "mongodb://" + dbUser + ":" + dbPswd + "@" + dbUrl + ":" + \
      str(dbPort) + "/" + dbName
  client = MongoClient(dbURI)
  print dbURI
  return client[dbName]

  
if __name__ == '__main__':
  """dbUrl = "kahana.mongohq.com"
  dbPort = '10056'
  dbName = 'IdeaGensTest'
  dbUser = 'sandbox'
  dbPswd = 'protolab1' """
  db = get_mongodb(ideagens['url'],
                   ideagens['port'],
                   ideagens['dbName'], 
                   ideagens['user'],
                   ideagens['pswd'])

  parts = db.participants.find()
  ideas = db.ideas.find()
  events = db.events.find()

  count = 0
  mturk_data = open('mturk_data.csv', "w")

  ideation_starts = []
  for event in events:
    part_id = event['participant']['_id']
    event_desc = event['description']
    event_time = event['time']
    if (event_desc == "Participant began ideation"):
      ideation_starts.append([part_id, event_time])

  start_times = dict(ideation_starts)

  mturk_data = open('mturk_data.csv', "w")
  mturk_data.write("part_ID, part_prime, idea_ID, idea_content, idea_time, ideation_start, rel_idea_time \n")
  for idea in ideas:
    prompt_ID = idea['prompt']['_id']

    part_ID = idea['participant']['_id']

    part_prime = idea['participant']['condition']['description'].split()
    part_prime = " ".join(part_prime[4:5])

    idea_ID = idea['_id']

    idea_content = idea['content'].replace(', ', '; ').replace('\n', ' ')

    idea_time = idea['time']
    idea_time = datetime.utcfromtimestamp(idea_time/1000.0)

    ideation_start_time = start_times[part_ID]

    rel_idea_time = idea_time - ideation_start_time
    mturk_data.write("%s, %s, %s, %s, %s, %s, %s \n" % (part_ID, part_prime, idea_ID, idea_content, idea_time, ideation_start_time, rel_idea_time))
  mturk_data.close()
