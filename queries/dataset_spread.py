# For all preprocessing methods on $D$, what is the change in dataset spread.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint
import time
import sys

def get_dataset_spread():
	# Get invalidated items for all preprocessing methods:
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
		{'$group': {'_id': '$attributes.operation_number', 'invalidated_entities': {'$addToSet': '$prov:entity'}}} \
		#{'$group': {'_id': '$prov:activity', 'invalidated_entities': {'$addToSet': '$prov:entity'}}} 
	])

	# Get new items for all preprocessing methods:
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
		invalid_items, new_items = get_dataset_spread()

		time2 = time.time()
		#text = '{:s} function took {:.3f} ms'.format('Dataset Spread', (time2-time1)*1000.0)
		text = '{:s} function took {:.3f} sec.'.format('Dataset Spread', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: dataset_spread.py <db_name>')
