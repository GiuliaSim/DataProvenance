# For all preprocessing methods on $D$, what is the change in dataset spread.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint
import time
import sys

def get_items():
	# Get invalidated items for all preprocessing methods:
	invalid_items = relations.aggregate([ \
		{'$match': {'prov:relation_type': 'wasInvalidatedBy'}}, \
		{'$group': {'_id': '$prov:activity', 'invalidated_entities': {'$addToSet': '$prov:entity'}}} 
	])

	# Get new items for all preprocessing methods:
	new_items = relations.aggregate([ \
		{'$match': {'prov:relation_type': 'wasGeneratedBy'}}, \
		{'$group': {'_id': '$prov:activity', 'new_entities': {'$addToSet': '$prov:entity'}}} 
	])

	invalid_items = list(invalid_items)
	new_items = list(new_items)

	return (invalid_items, new_items)

def get_dataset_spread(invalid_items, new_items):
	# Get all preprocessing methods id:
	act_new = [e['_id'] for e in new_items]
	act_invalid = [e['_id'] for e in invalid_items]
	acts_id = list(set(act_new + act_invalid))

	invalidated_entities = [e['invalidated_entities'] for e in invalid_items]
	invalidated_entities = [e for sublist in invalidated_entities for e in sublist]
	new_entities = [e['new_entities'] for e in new_items]
	new_entities = [e for sublist in new_entities for e in sublist]

	print('Number of invalidated items: ' + str(len(invalidated_entities)))
	print('Number of new items: ' + str(len(new_entities)))

	# Get the feature name of the invalidated entities
	invalid_feature = entities.aggregate([ \
		{'$match': {'identifier':{'$in':invalidated_entities}}}, \
		{'$project': {'invalid_feature': '$attributes.feature_name', '_id': 0}} \
	])
	invalid_feature = list(invalid_feature)
	invalid_feature = [d['invalid_feature'] for d in invalid_feature]

	# Get the feature name of the new entities
	new_feature = entities.aggregate([ \
		{'$match': {'identifier':{'$in':new_entities}}}, \
		{'$project': {'new_feature': '$attributes.feature_name', '_id': 0}} \
	])
	new_feature = list(new_feature)
	new_feature = [d['new_feature'] for d in new_feature]


	if invalidated_entities:
		print('Max Feature name on invlidated entities: ' + max(invalid_feature))
	if new_entities:
		print('Max Feature name on new entities: ' + max(new_feature))


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
		invalid_items, new_items = get_items()
		get_dataset_spread(invalid_items, new_items)
		time2 = time.time()

		text = '{:s} function took {:.3f} sec.'.format('Dataset Spread', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: dataset_spread.py <db_name>')
