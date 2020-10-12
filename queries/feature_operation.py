# Set of preprocessing methods that were applied to feature $D_{*j}$.
# Input: D_{*j}$

import pymongo
import pandas as pd
import pprint
import time
import sys
import random
import numpy as np

def get_random_feature():

	# Get output_entities collection
	output_entities = db['output_entities']

	# Get random document on output_entities collection
	random_ent = list(output_entities.aggregate([{'$sample': {'size': 1}}]))
	# Get feature_name
	feature_name = random_ent[0]['attributes']['feature_name']

	return feature_name

def get_dataset_operation(feature_name):
	return activities.find({'attributes.features_name': {'$regex': '.*' + feature_name + '*.'}})

if __name__ == "__main__":

	if len(sys.argv) == 2 :
		dbname = sys.argv[1]

		# Connect with MongoClient on the default host and port:
		client = pymongo.MongoClient('localhost', 27017)

		# Getting a Database:
		db = client[dbname]

		# Get activities mongodb collection:
		activities = db.activities
		entities = db.entities
		relations = db.relations

		feature_name = get_random_feature()
		#feature_name = 'workclass'
		#feature_name = 'checking'
		print('Feature Operation of: ' + feature_name)

		time1 = time.time()
		# Get the activities that were applied to feature:
		methods = get_dataset_operation(feature_name)

		#pprint.pprint(methods.explain())

		time2 = time.time()

		# Print description of input entities and preprocessing methods that created the element $d_{ij}$:
		for act in methods:
			pprint.pprint(act)

		executionTimeMillis = methods.explain()['executionStats']['executionTimeMillis']
		print('executionTimeMillis of Feature Operation: ' + str(executionTimeMillis))

		
		#text = '{:s} function took {:.3f} ms'.format('Feature Operation', (time2-time1)*1000.0)
		text = '{:s} function took {:.3f} sec.'.format('Feature Operation', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: feature_operation.py <db_name>')

