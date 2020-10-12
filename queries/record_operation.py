# Set of preprocessing methods that were applied to record, $D_{i*}$.
# Input: $D_{i*}$

import pymongo
import numpy as np
import pandas as pd
import pprint
import time
import sys
import random

def get_random_record():

	# Get output_entities collection
	output_entities = db['output_entities']

	# Get random document on output_entities collection
	random_ent = list(output_entities.aggregate([{'$sample': {'size': 1}}]))

	# Get feature_name
	record_id = random_ent[0]['attributes']['record_id']

	return record_id

def get_record_operation2(record_id):
	# Get list of activities id related to the record:
	out = entities.aggregate([
		{'$match': \
			{'attributes.record_id': record_id}
		}, \
	    {'$lookup': \
	    	{ \
	    		'from': 'relations', \
	    		'localField': 'identifier', \
	    		'foreignField': 'prov:entity', \
	    		'as': 'relations' \
	    	}
	    }, \
	    {'$unwind': '$relations'}, \
	    {'$group':{'_id':'$relations.prov:activity'}}, \
	    {'$project': {'activity_id': '$_id', '_id': 0}}
	])

	return out


def get_record_operation(record_id):
	# Get the entities of the record:
	entities_id = entities.find({'attributes.record_id': record_id}, {'identifier': 1, '_id': 0}).distinct('identifier')
	#print(len(entities_id))

	# Get list of activities id related to the record:
	out = relations.aggregate([ \
		{'$match':{ 'prov:entity':{'$in': entities_id}}}, \
		{'$group': {'_id': '$prov:activity', 'entities': {'$addToSet': '$prov:entity'}}}, \
	    {'$project': {'activity_id': '$_id', '_id': 0}}
	])

	return out


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

		record_id = get_random_record()

		print('Record Operation of: ' + record_id)

		time1 = time.time()

		# Get the activities id that were applied to record_id:
		acts = get_record_operation(record_id)

		time2 = time.time()

		# Print list of result activities identifier
		acts = list(acts)
		#pprint.pprint(acts)
		print('Number of activities: ' + str(len(acts)))

		text = '{:s} function took {:.3f} sec.'.format('Record Operation', (time2-time1))
		print(text)

		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: record_operation.py <db_name>')
