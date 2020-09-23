# The preprocessing method that deleted the record, $D_{i*}$.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint
import time
import sys

def get_preprocessing_methods(invalid_records):
	# Get the preprocessing methods that deleted the records
	out = entities.aggregate([ \
		{'$match': {'attributes.record_id':{'$in':invalid_records}}},
		{'$lookup': \
	    	{ \
	    		'from': 'relations', \
	    		'let': { 'entity': '$identifier'}, \
	    		'pipeline': [ \
	    			{ '$match': \
	    				{ '$expr': \
	    					{'$and': [\
		    					{ '$eq': [ '$prov:entity',  '$$entity' ] }, \
		    					{ '$eq': [ '$prov:relation_type',  'wasInvalidatedBy' ] } \
	    					]} \
	    				} \
	    			}, \
	    		], \
	    		'as': "invalidatedBy" \
	    	} \
	    }, \
		{'$group': {'_id': '$attributes.record_id', 'activities': {'$addToSet': '$invalidatedBy.prov:activity'}}}, \
	])
	return out

def get_invalid_records():
	'''All $D_{i*}$ that were deleted'''
	# Get invalidated entities id:
	invalid_ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')
	
	# Group entities by record_id
	# Get deleted records: if all record entities are invalidated, the record is deleted
	invalid_records = entities.aggregate([ \
		{'$group': {'_id': '$attributes.record_id', 'entities': {'$addToSet': '$identifier'}}}, \
		{'$match': {'entities': {'$not': {'$elemMatch': {'$nin': invalid_ents_id}}}}}, \
		{'$group': {'_id': 'null', 'records': {'$push':'$_id'}}}, \
		{'$project':{'records':1,'_id': 0}}
	])

	invalid_records = list(invalid_records)[0]['records'] if len(list(invalid_records)) > 0 else []

	return invalid_records


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

		# Get the recordif that were deleted:
		invalid_records = get_invalid_records()
		# Get preprocessing method that deleted the records:
		methods = get_preprocessing_methods(invalid_records)


		# Get preprocessing method that deleted the records:
		#invalid_records = get_invalid_records()

		time2 = time.time()

		print('PREPROCESSING METHODS THAT DELETED RECORDS:')
		for r in methods:
			pprint.pprint(r)

		text = '{:s} function took {:.3f} sec.'.format('Record Invalidation', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: record_invalidation.py <db_name>')
