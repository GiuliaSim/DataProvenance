# Set of preprocessing methods that were applied to feature, $D_{*j}$.
# Input: $d_{ij}$

import pymongo
import numpy as np
import pandas as pd
import pprint
import time
import sys
import random

def get_random_item():
	
	# Get output_entities collection
	output_entities = db['output_entities']

	# Get random document on output_entities collection
	random_ent = list(output_entities.aggregate([{'$sample': {'size': 1}}]))
	
	# Get feature_name
	entity_id = random_ent[0]['identifier']

	return entity_id


def get_item_operation2(entity_id):
	# Get activities related to entity_ids:
	acts = relations.find({'prov:entity': {'$in': entity_id}}, {'prov:activity': 1, '_id': 0}).distinct('prov:activity')
	# Get entities used to generate entity_ids:
	used_ents = relations.find({'prov:generatedEntity': {'$in': entity_id}}, {'prov:usedEntity': 1, '_id': 0}).distinct('prov:usedEntity')
	if used_ents:
		return acts + get_item_operation(used_ents)
	else:
		return acts

def get_item_operation(entity_id):
	# Get activities related to entity_ids:
	entity = entities.find_one({'identifier': entity_id})
	index = entity['attributes']['index']
	feature_name = entity['attributes']['feature_name']

	entities_id = entities.find({'attributes.index': index, 'attributes.feature_name': feature_name}, {'identifier': 1, '_id': 0}).distinct('identifier')

	acts = relations.find({'prov:entity': {'$in': entities_id}}, {'prov:activity': 1, '_id': 0}).distinct('prov:activity')

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

		entity_id = get_random_item()
		
		print('Item operation of: ' + entity_id)

		#relations.create_index('prov:entity')
		#relations.create_index('prov:generatedEntity')

		time1 = time.time()
		# Get the activities id that were applied to element:
		#acts = get_item_operation([entity_id])
		acts = get_item_operation(entity_id)

		time2 = time.time()

		#pprint.pprint(acts)
		print('Number of activities: ' + str(len(acts)))

		text = '{:s} function took {:.3f} sec.'.format('Item Operation', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: item_operation.py <db_name>')
