# Set of all $D_{i*}$, $D_{*j}$, $d_{ij}$ that were deleted.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint
import time
import sys

def get_invalid_items():
	'''All $d_{ij}$ that were deleted'''
	# Get the feature name of the output_entities
	output_features = output_entities.aggregate([ \
		{'$group': {'_id': '$attributes.feature_name'}}
	],allowDiskUse=True)
	output_features = [i['_id'] for i in output_features]

	# Get the indexes of the output_entities
	output_indexes = output_entities.aggregate([ \
		{'$group': {'_id': '$attributes.index'}}
	],allowDiskUse=True)
	output_indexes = [i['_id'] for i in output_indexes]

	invalid_items = entities.aggregate([
		{'$match': {'$or': [
			{'attributes.feature_name': { '$nin': output_features }}, 
			{'attributes.index': { '$nin': output_indexes }}
		]}},
		{'$group': {
			'_id': {'feature_name':'$attributes.feature_name', 'index':'$attributes.index'},
			'maxInstance': {'$max':'$attributes.instance'},
			#'identifier': {'$first': '$identifier'}
			#'entity': {'$push':'$$ROOT'}
		}}
	],allowDiskUse = True)

	return invalid_items

def get_invalid_features():
	'''All $D_{*j}$ that were deleted'''
	# Get the feature name of the output_entities
	output_features = output_entities.aggregate([ \
		{'$group': {'_id': '$attributes.feature_name', 'ids': {'$sum': 1}}}
	],allowDiskUse=True)
	output_features = [i['_id'] for i in output_features]

	# Get all feature_name of the dataset
	all_features = entities.aggregate([
		{'$group': {'_id': '$attributes.feature_name'}}
	])
	all_features = [i['_id'] for i in all_features]

	# Get the feature name of the invalidated entities
	invalid_features = diff(all_features, output_features)

	return invalid_features


def get_invalid_records():
	'''All $D_{i*}$ that were deleted'''
	# Get the indexes of the output_entities
	output_indexes = output_entities.aggregate([ \
		{'$group': {'_id': '$attributes.index'}}
	],allowDiskUse=True)
	output_indexes = [i['_id'] for i in output_indexes]

	# Get all feature_name of the dataset
	all_indexes = entities.aggregate([
		{'$group': {'_id': '$attributes.index'}}
	])
	all_indexes = [i['_id'] for i in all_indexes]

	# Get the feature name of the invalidated entities
	invalid_indexes = diff(all_indexes, output_indexes)

	return invalid_indexes

if __name__ == "__main__":

	if len(sys.argv) == 2 :
		dbname = sys.argv[1]

		# Connect with MongoClient on the default host and port:
		client = pymongo.MongoClient('localhost', 27017)

		# Getting a Database:
		db = client[dbname]

		# Get entities, activities and relations mongodb collection:
		entities = db.entities
		#activities = db.activities
		#relations = db.relations
		output_entities = db['output_entities']

		diff = lambda l1, l2: [x for x in l1 if x not in l2]


		time1 = time.time()

		# Get the entities that were deleted:
		invalid_ents = get_invalid_items()

		# Get the features name that were deleted:
		invalid_features = get_invalid_features()

		# Get the record_id that were deleted:
		invalid_records = get_invalid_records()
		
		time2 = time.time()

		invalid_ents_count = len(list(invalid_ents))
		print('Count of invalidated entities: ' + str(invalid_ents_count))

		# Print deleted features names
		#for elem in invalid_features:
			#pprint.pprint(elem)


		invalid_features_count = len(list(invalid_features))
		print('Count of invalidated features: ' + str(invalid_features_count))

		# Print deleted record id
		#for elem in invalid_records:
			#pprint.pprint(elem)

		invalid_records_count = len(list(invalid_records))
		print('Count of invalidated records: ' + str(invalid_records_count))

		text = '{:s} function took {:.3f} sec.'.format('Invalidation', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: invalidation.py <db_name>')

