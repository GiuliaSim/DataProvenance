# Set of preprocessing methods applied to $D$, and the features, $D_{*j}$, they affect.
# Input: D - dataframe

import pymongo
import pandas as pd
import sys
import time


def all_transformations(db):
	activities = db.activities

	all_act = {}
	for act in activities.find():
		function_name = act['attributes']['function_name']
		all_act.setdefault(function_name,[]).append(act['attributes']['features_name'])

	for k, v in all_act.items():
		print(k, ":", v)

if __name__ == "__main__":

	if len(sys.argv) == 2 :
		dbname = sys.argv[1]

		# Connect with MongoClient on the default host and port:
		client = pymongo.MongoClient('localhost', 27017)
		
		# Getting a Database:
		db = client[dbname]
		
		time1 = time.time()
		all_transformations(db)
		time2 = time.time()
		#text = '{:s} function took {:.3f} ms'.format('All Transformations', (time2-time1)*1000.0)
		text = '{:s} function took {:.3f} sec.'.format('All Transformations', (time2-time1))
		print(text)

		# Close Mongodb connection:
		client.close()

	else:
		print('[ERROR] usage: create_mongodb.py <db_name> <files_path>')

