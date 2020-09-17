# Set of preprocessing methods that were applied to feature, $D_{*j}$.
# Input: $d_{ij}$

import pymongo
import pandas as pd
import pprint
import time
import sys
import random

def get_item_operation(entity_id):
	acts = relations.find({'prov:entity': {'$in': entity_id}}, {'prov:activity': 1, '_id': 0}).distinct('prov:activity')
	used_ents = relations.find({'prov:generatedEntity': {'$in': entity_id}}, {'prov:usedEntity': 1, '_id': 0}).distinct('prov:usedEntity')
	if used_ents:
		return acts + get_item_operation(used_ents)
	else:
		return acts

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

		# Get invalidated entities id
		invalidated_ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')

		# Get random element identifier $d_{ij}$:
		output_entities = list(entities.aggregate([
			{'$match': {'identifier': {'$nin': invalidated_ents_id}}}
		]))

		rand_num = random.randint(0,len(output_entities))

		entity_id = output_entities[rand_num]['identifier']
		
		print('Item operation of: ' + entity_id)

		time1 = time.time()
		# Get the activities id that were applied to element:
		acts = get_item_operation([entity_id])

		# Find mongodb documents from identifier list:
		methods = activities.find({'identifier':{'$in':acts}})

		for m in methods:
			pprint.pprint(m)

		# Print description of input entities and preprocessing methods that created the element $d_{ij}$:
		#pprint.pprint(methods.explain())

		time2 = time.time()
		#text = '{:s} function took {:.3f} ms'.format('Item Operation', (time2-time1)*1000.0)
		text = '{:s} function took {:.3f} sec.'.format('Item Operation', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: item_operation.py <db_name>')
