# Set of preprocessing methods that were applied to feature $D_{*j}$.
# Input: D_{*j}$

import pymongo
import pandas as pd
import pprint
import time
import sys
import random
import numpy as np

def get_output_entities2():
	if ('output_entities' not in db.list_collection_names()):
		# Create Index on relations collection
		relations.create_index('prov:relation_type')
		relations.create_index('prov:entity')
		
		# Get invalidated entities id
		invalidated_ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')

		# Create Index on entities collection
		entities.create_index('identifier')
		entities.create_index('attributes.features_name')
		entities.create_index('attributes.record_id')

		# Get output entities
		out = entities.aggregate([
			{'$match': {'identifier': {'$nin': invalidated_ents_id}}}, \
			{'$out': 'output_entities'}
		], allowDiskUse=True)

def get_output_entities():
	if ('output_entities' not in db.list_collection_names()):
		# Create Index on relations collection
		relations.create_index('prov:relation_type')
		relations.create_index('prov:entity')

		# Get invalidated entities id
		invalidated_ents_id = relations.aggregate([ \
			{'$match': {'prov:relation_type': 'wasInvalidatedBy'}}, \
			{'$group': {'_id': '$prov:entity'}}
		])
		invalidated_ents_id = [i['_id'] for i in list(invalidated_ents_id)]
		
		# Create Index on entities collection
		entities.create_index('identifier')
		entities.create_index('attributes.features_name')

		# Split invalidates entity ids in three
		a,b,c = np.array_split(invalidated_ents_id, 3)
		a = list(a)
		b = list(b)
		c = list(c)
		
		# Filter entities, delete the entities in list a:
		out = entities.aggregate([
			{'$match': {'identifier': {'$nin': a}}}, \
			{'$out': 'output_entities'}
		], allowDiskUse=True)

		output_entities = db['output_entities']

		# Filter entities, delete the entities in list b:
		out = output_entities.aggregate([
			{'$match': {'identifier': {'$nin': b}}}, \
			{'$out': 'output_entities'}
		], allowDiskUse=True)

		# Filter entities, delete the entities in list c:
		out = output_entities.aggregate([
			{'$match': {'identifier': {'$nin': c}}}, \
			{'$out': 'output_entities'}
		], allowDiskUse=True)

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

		time1 = time.time()
		# Get the activities that were applied to feature:
		get_output_entities()

		time2 = time.time()

		text = '{:s} function took {:.3f} sec.'.format('Output Entities', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: feature_operation.py <db_name>')

