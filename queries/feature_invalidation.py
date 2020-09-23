# The preprocessing method that deleted the feature, $D_{*j}$.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint
import time
import sys

def get_preprocessing_methods(invalid_features):
	# Get the preprocessing methods that deleted the features
	out = activities.aggregate([ \
		{'$match': {'attributes.features_name':{'$in':invalid_features}}},
		{'$sort': {'attributes.operation_number':-1}},
		{'$group': {'_id': '$attributes.features_name', 'function_name': {'$first': '$attributes.function_name'}}}, \
		{'$project': {'_id':0,'feature_name':'$_id', 'function_name':1}} \
	])
	return out


def get_invalid_features():
	'''All $D_{*j}$ that were deleted'''
	# Get invalidated entities id:
	invalid_ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')

	# Group entities by feature_name
	# Get deleted features: if all feature entities are invalidated, the feature is deleted
	invalid_features = entities.aggregate([ \
		{'$group': {'_id': '$attributes.feature_name', 'entities': {'$addToSet': '$identifier'}}}, \
		{'$match': {'entities': {'$not': {'$elemMatch': {'$nin': invalid_ents_id}}}}}, \
		{'$group': {'_id': 'null', 'features': {'$push':'$_id'}}}, \
		{'$project':{'features':1,'_id': 0}}
	])

	invalid_features = list(invalid_features)[0]['features'] if len(list(invalid_features)) > 0 else []

	return invalid_features


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

		# Get the features name that were deleted:
		invalid_features = get_invalid_features()
		# Get preprocessing method that deleted the features:
		methods = get_preprocessing_methods(invalid_features)

		time2 = time.time()

		print('PREPROCESSING METHODS THAT DELETED FEATURES:')
		for f in methods:
			pprint.pprint(f)

		text = '{:s} function took {:.3f} sec.'.format('Invalidation', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: feture_invalidation.py <db_name>')

