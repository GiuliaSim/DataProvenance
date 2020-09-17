# Set of preprocessing methods that were applied to feature $D_{*j}$.
# Input: D_{*j}$

import pymongo
import pandas as pd
import pprint
import time
import sys
import random

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

		# Get invalidated entities id
		invalidated_ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')

		# Get random element identifier $d_{ij}$:
		output_entities = list(entities.aggregate([
			{'$match': {'identifier': {'$nin': invalidated_ents_id}}}
		]))

		rand_num = random.randint(0,len(output_entities))

		# Feature name of $D_{*j}$:
		feature_name = output_entities[rand_num]['attributes']['feature_name']
		#feature_name = 'checking'

		print('Feature Operation of: ' + feature_name)

		time1 = time.time()
		# Get the activities that were applied to feature:
		methods = get_dataset_operation(feature_name)

		# Print description of input entities and preprocessing methods that created the element $d_{ij}$:
		#pprint.pprint(methods.explain())

		for act in methods:
			pprint.pprint(act)

		time2 = time.time()
		#text = '{:s} function took {:.3f} ms'.format('Dataset Operation', (time2-time1)*1000.0)
		text = '{:s} function took {:.3f} sec.'.format('Dataset Operation', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: dataset_operation.py <db_name>')

