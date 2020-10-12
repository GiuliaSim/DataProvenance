# The preprocessing method that deleted the item, $d_{ij}$.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint
import time
import sys


def get_preprocessing_methods():
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

	# Get invalidated items:
	#invalid_items = entities.aggregate([
	#	{'$match': {'$or': [
	#		{'attributes.feature_name': { '$nin': output_features }}, 
	#		{'attributes.index': { '$nin': output_indexes }}
	#	]}}
	#],allowDiskUse = True)
	#invalid_items = [i['identifier'] for i in invalid_items]

	# Get preprocessing methods
	#out = relations.aggregate([ \
	#	{'$match': {'prov:relation_type':'wasInvalidatedBy', 'prov:entity': {'$in': invalid_items}}}, \
	#	{'$group': {'_id': '$prov:activity', 'entities': {'$addToSet': '$prov:entity'}}}, \
		#{'$out':'item_invalidation'}
	#])

	out = entities.aggregate([
		{'$match': {'$or': [
			{'attributes.feature_name': { '$nin': output_features }}, 
			{'attributes.index': { '$nin': output_indexes }}
		]}},
	    {'$lookup': \
	    	{ \
	    		'from': 'relations', \
	    		'localField': 'identifier', \
	    		'foreignField': 'prov:entity', \
	    		'as': 'relations' \
	    	}
	    }, \
	    {'$unwind': '$relations'}, \
	    {'$match': {'relations.prov:relation_type': 'wasInvalidatedBy'}},
	    {'$group': {'_id': '$relations.prov:activity', 'entities': {'$addToSet': '$identifier'}}}
	], allowDiskUse=True)

	return out


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
		# Get preprocessing method that deleted the items:
		methods = get_preprocessing_methods()
		#print('Result saved in item_invalidation collection.')

		methods = list(methods)

		#for method in methods:
		#	print(method['_id'])

		print('Number of preprocessing methods: ' + str(len(methods)))

		

		time2 = time.time()
		#text = '{:s} function took {:.3f} ms'.format('Item Invalidation', (time2-time1)*1000.0)
		text = '{:s} function took {:.3f} sec.'.format('Item Invalidation', (time2-time1))
		print(text)

		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: item_invalidation.py <db_name>')

