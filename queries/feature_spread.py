# For all preprocessing methods applied on a feature, $D_{*j}$, in $D$, what is the change in feature spread.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint
import time
import sys

def get_dataset_spread(invalid_items, new_items):
	invalid_items =list(invalid_items)
	new_items =list(new_items)

	# Iterate all preprocessing methods related to feature $D_{*j}$:
	for i in invalid_items:
		# Get the operation number:
		operation_number = i['_id'][0]
		# Get the related invalidated entities id:
		invalidated_entities = i['invalidated_entities']
		# Get the related new entities id:
		new_entities = [e['new_entities'] for e in new_items if e['_id'][0] == operation_number][0]

		# Get the values of the invalidated entities
		invalid_values = entities.aggregate([ \
			{'$match': {'identifier':{'$in':invalidated_entities}}}, \
			{'$project': {'invalid_values': '$attributes.value', '_id': 0}} \
		])
		invalid_values = list(invalid_values)
		invalid_values = [d['invalid_values'] for d in invalid_values]

		# Get the values of the new entities
		new_values = entities.aggregate([ \
			{'$match': {'identifier':{'$in':new_entities}}}, \
			{'$project': {'new_values': '$attributes.value', '_id': 0}} \
		])
		new_values = list(new_values)
		new_values = [d['new_values'] for d in new_values]

		print('------------------------------------------')
		print('Preprocessing methos: ' + operation_number)
		print('Max invalid_values ' + max(invalid_values))
		print('Max new_values ' + max(new_values))

def get_items(feature_name):
	# Get invalidated items for all preprocessing methods related to feature $D_{*j}$:
	invalid_items = relations.aggregate([ \
		{'$match': {'prov:relation_type': 'wasInvalidatedBy'}}, \
		{'$lookup': \
	    	{ \
	    		'from': 'activities', \
	    		'let': { 'activity': '$prov:activity'}, \
	    		'pipeline': [ \
	    			{ '$match': \
	    				{ '$expr': \
		    				{ '$eq': [ '$identifier',  '$$activity' ] }, \
	    				} \
	    			}, \
	    		], \
	    		'as': "activity" \
	    	} \
	    }, \
		{'$match': {'activity.attributes.features_name': {'$regex': '.*' + feature_name + '*.'}}}, \
		{'$group': {'_id': '$activity.attributes.operation_number', 'invalidated_entities': {'$addToSet': '$prov:entity'}}} \
	])

	# Get new items for all preprocessing methods related to feature $D_{*j}$:
	new_items = relations.aggregate([ \
		{'$match': {'prov:relation_type': 'wasGeneratedBy'}}, \
		{'$lookup': \
	    	{ \
	    		'from': 'activities', \
	    		'let': { 'activity': '$prov:activity'}, \
	    		'pipeline': [ \
	    			{ '$match': \
	    				{ '$expr': \
		    				{ '$eq': [ '$identifier',  '$$activity' ] }, \
	    				} \
	    			}, \
	    		], \
	    		'as': "activity" \
	    	} \
	    }, \
		{'$match': {'activity.attributes.features_name': {'$regex': '.*' + feature_name + '*.'}}}, \
		{'$group': {'_id': '$activity.attributes.operation_number', 'new_entities': {'$addToSet': '$prov:entity'}}} \
	])

	return (invalid_items, new_items)

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
		invalid_items, new_items = get_items('checking')

		#print('---INVALID ITEMS-------------------------------------------')
		#for i in invalid_items:
		#	pprint.pprint(i)

		#print('---NEW ITEMS-----------------------------------------------')
		#for i in new_items:
		#	pprint.pprint(i)

		get_dataset_spread(invalid_items, new_items)

		time2 = time.time()
		#text = '{:s} function took {:.3f} ms'.format('Feature Spread', (time2-time1)*1000.0)
		text = '{:s} function took {:.3f} sec.'.format('Feature Spread', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: feture_spread.py <db_name>')
