# The preprocessing method that deleted the feature, $D_{*j}$.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint
import time
import sys

def get_preprocessing_methods(invalid_features):

	regex = '|'.join(invalid_features)

	# Get the preprocessing methods that deleted the features
	out = activities.aggregate([ \
		{'$match': {'attributes.features_name':{'$regex':'.*' + regex + '*.'}}},
		#{'$sort': {'attributes.operation_number':-1}},
		#{'$group': {'_id': '$attributes.features_name', 'function_name': {'$first': '$attributes.function_name'}}}, \
		#{'$project': {'_id':0,'feature_name':'$_id', 'function_name':1}} \
	])
	return out



def get_invalid_features():
	'''All $D_{*j}$ that were deleted'''
	diff = lambda l1, l2: [x for x in l1 if x not in l2]
	
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
		output_entities = db['output_entities']

		time1 = time.time()

		# Get the features name that were deleted:
		invalid_features = get_invalid_features()
		# Get preprocessing method that deleted the features:
		methods = get_preprocessing_methods(invalid_features)

		time2 = time.time()

		#print('PREPROCESSING METHODS THAT DELETED FEATURES:')
		#for f in methods:
		#	pprint.pprint(f)

		#print('Number of deleted features: ' + str(len(invalid_features)))
		print('Number of preprocessing methods: ' + str(len(list(methods))))

		text = '{:s} function took {:.3f} sec.'.format('Feature Invalidation', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: feture_invalidation.py <db_name>')

