# Set of all $D_{i*}$, $D_{*j}$, $d_{ij}$ that were deleted.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint
import time
import sys

def get_invalid_items():
	'''All $d_{ij}$ that were deleted'''
	ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')

	invalid_ents = entities.aggregate([ \
		{'$group': {'_id': {'feature_name':'$attributes.feature_name','index':'$attributes.index'}, 'entities': {'$addToSet': '$identifier'}}}, \
		{'$match': {'entities': {'$not': {'$elemMatch': {'$nin': ents_id}}}}}, \
		{'$project': {'item': '$_id', '_id': 0}}
	])

	#invalid_ents = entities.find({'identifier': {'$in': ents_id}})
	return invalid_ents

def get_invalid_features():
	'''All $D_{*j}$ that were deleted'''
	# Get invalidated entities id:
	invalid_ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')
	
	# Group entities by feature_name
	# Get deleted features: if all feature entities are invalidated, the feature is deleted
	invalid_features = entities.aggregate([ \
		{'$group': {'_id': '$attributes.feature_name', 'entities': {'$addToSet': '$identifier'}}}, \
		{'$match': {'entities': {'$not': {'$elemMatch': {'$nin': invalid_ents_id}}}}}, \
		{'$project': {'feature_name': '$_id', '_id': 0}}
	])

	return invalid_features


def get_invalid_records():
	'''All $D_{i*}$ that were deleted'''
	# Get invalidated entities id:
	invalid_ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')
	
	# Group entities by record_id
	# Get deleted records: if all record entities are invalidated, the record is deleted
	invalid_records = entities.aggregate([ \
		{'$group': {'_id': '$attributes.record_id', 'entities': {'$addToSet': '$identifier'}}}, \
		{'$match': {'entities': {'$not': {'$elemMatch': {'$nin': invalid_ents_id}}}}}, \
		{'$project': {'record_id': '$_id', '_id': 0}}
	])

	return invalid_records

if __name__ == "__main__":

	if len(sys.argv) == 2 :
		dbname = sys.argv[1]

		# Connect with MongoClient on the default host and port:
		client = pymongo.MongoClient('localhost', 27017)

		# Getting a Database:
		db = client[dbname]

		# Get entities, activities and relations mongodb collection:
		entities = db.entities
		activities = db.activities
		relations = db.relations

		time1 = time.time()

		# Get the entities that were deleted:
		invalid_ents = get_invalid_items()
		

		# Get the features name that were deleted:
		invalid_features = get_invalid_features()
		

		# Get the record_id that were deleted:
		invalid_records = get_invalid_records()

		time2 = time.time()

		for item in invalid_ents:
			pprint.pprint(item)

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

