# Set of preprocessing methods that were applied to record, $D_{i*}$.
# Input: $D_{i*}$

import pymongo
import pandas as pd
import pprint
import time
import sys
import random

def get_record_operation(record_id):
	# Get list of activities id related to the record:
	out = entities.aggregate([
		{'$match': \
			{'attributes.record_id': record_id}
		}, \
	    {'$lookup': \
	    	{ \
	    		'from': 'relations', \
	    		'let': { 'entity': '$identifier'}, \
	    		'pipeline': [ \
	    			{ '$match': \
	    				{ '$expr': \
	    					{ '$eq': [ '$prov:entity',  '$$entity' ] }
	    				} \
	    			}, \
	    		], \
	    		'as': "relations" \
	    	} \
	    }, \
	    {'$project': {'activities': '$relations.prov:activity', '_id': 0}} \
	])

	acts = []
	for elem in out:
		acts += elem['activities']

	return acts

if __name__ == "__main__":

	if len(sys.argv) == 2 :
		dbname = sys.argv[1]

		# Connect with MongoClient on the default host and port:
		client = pymongo.MongoClient('localhost', 27017)

		# Getting a Database:
		db = client[dbname]

		# Get entities and activities mongodb collection:
		entities = db.entities
		activities = db.activities
		relations = db.relations

		# Get invalidated entities id
		invalidated_ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')

		# Get random element identifier $d_{ij}$:
		output_entities = list(entities.aggregate([
			{'$match': {'identifier': {'$nin': invalidated_ents_id}}}
		]))

		rand_num = random.randint(0,len(output_entities))

		# Record identifier $D_{i*}$:
		record_id = output_entities[rand_num]['attributes']['record_id']

		print('Record Operation of: ' + record_id)

		time1 = time.time()
		# Get the activities id that were applied to record_id:
		acts = get_record_operation(record_id)

		# Find mongodb documents from atcs list:
		methods = activities.find({'identifier':{'$in': acts}})

		for m in methods:
			pprint.pprint(m)

		time2 = time.time()
		#text = '{:s} function took {:.3f} ms'.format('Record Operation', (time2-time1)*1000.0)
		text = '{:s} function took {:.3f} sec.'.format('Record Operation', (time2-time1))
		print(text)

		# Print description of preprocessing methods that were applied to record, $D_{i*}$:
		#pprint.pprint(methods.explain())
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: record_operation.py <db_name>')
