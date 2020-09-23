# The preprocessing method that deleted the item, $d_{ij}$.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint
import time
import sys


def get_preprocessing_methods():
	# Get invalidated entities:
	ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')

	# Get identifiers of the invalidated element $d_{ij}$:
	invalid_ents = entities.aggregate([ \
		{'$group': {'_id': {'feature_name':'$attributes.feature_name','index':'$attributes.index'}, 'entities': {'$addToSet': '$identifier'}}}, \
		{'$match': {'entities': {'$not': {'$elemMatch': {'$nin': ents_id}}}}}, \
		{'$unwind': '$entities'}, \
		{'$group': {'_id': 'null', 'idetifires': {'$push':'$entities'}}}, \
		{'$project':{'idetifires':1,'_id': 0}}
	])

	# Get list of identifiers
	invalid_ents_ids = list(invalid_ents)[0]['idetifires']

	# Get preprocessing methods
	out = relations.aggregate([ \
		{'$match': {'prov:relation_type':'wasInvalidatedBy', 'prov:entity': {'$in': invalid_ents_ids}}}, \
		{'$group': {'_id': '$prov:activity', 'entities': {'$addToSet': '$prov:entity'}}}, \
		{'$out':'item_invalidation'}
	])

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

		time1 = time.time()
		# Get preprocessing method that deleted the items:
		methods = get_preprocessing_methods()
		print('Result saved in item_invalidation collection.')

		#print('PREPROCESSING METHODS THAT DELETED ITEMS:')
		#for i in methods:
		#	pprint.pprint(i)

		time2 = time.time()
		#text = '{:s} function took {:.3f} ms'.format('Item Invalidation', (time2-time1)*1000.0)
		text = '{:s} function took {:.3f} sec.'.format('Item Invalidation', (time2-time1))
		print(text)

		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: item_invalidation.py <db_name>')

