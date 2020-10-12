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
	diff = lambda l1, l2: [x for x in l1 if x not in l2]

	# Get the indexes of the output_entities
	output_indexes = output_entities.aggregate([ \
		{'$group': {'_id': '$attributes.index'}}
	],allowDiskUse=True)
	output_indexes = [i['_id'] for i in output_indexes]

	# Get all feature_name of the dataset
	all_indexes = entities.aggregate([
		{'$group': {
			'_id':  '$attributes.index',
			'record_id': {'$first':'$attributes.record_id'},
		}},
	])
	all_indexes = [i['_id'] for i in all_indexes]

	# Get the feature name of the invalidated entities
	invalid_indexes = diff(all_indexes, output_indexes)

	invalid_records = entities.aggregate([
		{'$match': {'attributes.index': {'$in': invalid_indexes}}},
		{'$group': {'_id': '$attributes.record_id'}}
	])
	invalid_records = [i['_id'] for i in invalid_records]

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
		output_entities = db['output_entities']

		time1 = time.time()

		# Get the recordif that were deleted:
		invalid_records = get_invalid_records()
		# Get preprocessing method that deleted the records:
		methods = get_preprocessing_methods(invalid_records)


		# Get preprocessing method that deleted the records:
		#invalid_records = get_invalid_records()

		time2 = time.time()

		#print('PREPROCESSING METHODS THAT DELETED RECORDS:')
		#for r in methods:
		#	pprint.pprint(r)
		
		#print('Number of deleted records: ' + str(len(invalid_records)))
		print('Number of preprocessing methods: ' + str(len(list(methods))))

		text = '{:s} function took {:.3f} sec.'.format('Record Invalidation', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: record_invalidation.py <db_name>')
