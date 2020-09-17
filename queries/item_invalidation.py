# The preprocessing method that deleted the item, $d_{ij}$.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint
import time
import sys

def get_preprocessing_methods():
	invalid_items = relations.aggregate([ \
		{'$match': {'prov:relation_type':'wasInvalidatedBy'}},
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
		{'$project': {'_id': 0, 'entity_id':'$prov:entity', 'preprocessing_method':'$activity.attributes.function_name'}}, \
		#{'$group': {'_id': '$preprocessing_method', 'invalidated_entities': {'$addToSet': '$entity_id'}}} \
		{'$out':'item_invalidation'}
	])

	return invalid_items

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
		# Get preprocessing method that deleted the items:
		invalid_items = get_preprocessing_methods()
		print('Result saved in item_invalidation collection.')

		#print('PREPROCESSING METHODS THAT DELETED ITEMS:')

		#for i in invalid_items:
		#	pprint.pprint(i)

		time2 = time.time()
		#text = '{:s} function took {:.3f} ms'.format('Item Invalidation', (time2-time1)*1000.0)
		text = '{:s} function took {:.3f} sec.'.format('Item Invalidation', (time2-time1))
		print(text)

		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: item_invalidation.py <db_name>')

