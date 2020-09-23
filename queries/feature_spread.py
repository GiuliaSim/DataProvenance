# For all preprocessing methods applied on a feature, $D_{*j}$, in $D$, what is the change in feature spread.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint
import time
import sys
import random

def get_random_feature():
	# Get invalidated entities id
	invalidated_ents_id = relations.find({'prov:relation_type': 'wasInvalidatedBy'}, {'prov:entity': 1, '_id': 0}).distinct('prov:entity')

	# Get random element identifier $d_{ij}$:
	output_entities = list(entities.aggregate([
		{'$match': {'identifier': {'$nin': invalidated_ents_id}}}
	]))

	rand_num = random.randint(0,len(output_entities))

	# Feature name of $D_{*j}$:
	feature_name = output_entities[rand_num]['attributes']['feature_name']
	#feature_name = 'checking'

	return feature_name

def get_dataset_spread2(invalid_items, new_items):
	# Get all preprocessing methods id:
	act_new = [e['_id'] for e in new_items]
	act_invalid = [e['_id'] for e in invalid_items]
	acts_id = list(set(act_new + act_invalid))

	# Iterate all preprocessing methods related to feature $D_{*j}$:
	for i in acts_id:
		# Get the activity identifier:
		act_identifier = i
		# Get the related invalidated entities id:
		invalidated_entities = [e['invalidated_entities'] for e in invalid_items if e['_id'] == act_identifier]

		if invalidated_entities:
			invalidated_entities = invalidated_entities[0]
			# Get the values of the invalidated entities
			invalid_values = entities.aggregate([ \
				{'$match': {'identifier':{'$in':invalidated_entities}}}, \
				{'$project': {'invalid_values': '$attributes.value', '_id': 0}} \
			])
			invalid_values = list(invalid_values)
			invalid_values = [d['invalid_values'] for d in invalid_values]

		# Get the related new entities id:
		new_entities = [e['new_entities'] for e in new_items if e['_id'] == act_identifier]

		if new_entities:
			new_entities = new_entities[0]
			# Get the values of the new entities
			new_values = entities.aggregate([ \
				{'$match': {'identifier':{'$in':new_entities}}}, \
				{'$project': {'new_values': '$attributes.value', '_id': 0}} \
			])
			new_values = list(new_values)
			new_values = [d['new_values'] for d in new_values]

		print('------------------------------------------')
		print('Activity identifier: ' + str(act_identifier))
		print('Number of invalidated items: ' + str(len(invalidated_entities)))
		print('Number of new items: ' + str(len(new_entities)))
		if invalidated_entities:
			print('Max invalid_values ' + max(invalid_values))
		if new_entities:
			print('Max new_values ' + max(new_values))


def get_items2(feature_name):
	# Get activities relatetd to feature_name
	acts = activities.find({'attributes.features_name': {'$regex': '.*' + feature_name + '*.'}}, {'identifier': 1, '_id': 0}).distinct('identifier')

	# Get invalidated items for all preprocessing methods related to feature $D_{*j}$:
	invalid_items = relations.aggregate([ \
		{'$match': {'prov:relation_type': 'wasInvalidatedBy', 'prov:activity': {'$in': acts}}}, \
		{'$group': {'_id': '$prov:activity', 'invalidated_entities': {'$addToSet': '$prov:entity'}}} 
	])

	# Get new items for all preprocessing methods related to feature $D_{*j}$:
	new_items = relations.aggregate([ \
		{'$match': {'prov:relation_type': 'wasGeneratedBy', 'prov:activity': {'$in': acts}}}, \
		{'$group': {'_id': '$prov:activity', 'new_entities': {'$addToSet': '$prov:entity'}}} 
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

		feature_name = get_random_feature()

		print('Feature Spread of: ' + feature_name)

		time1 = time.time()
		invalid_items, new_items = get_items2(feature_name)
		invalid_items = list(invalid_items)
		new_items = list(new_items)

		if ((invalid_items) or (new_items)):
			get_dataset_spread2(invalid_items, new_items)
		else:
			print('No operation on '+ feature_name )

		time2 = time.time()

		text = '{:s} function took {:.3f} sec.'.format('Feature Spread', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: feture_spread.py <db_name>')



def get_dataset_spread(invalid_items, new_items):
	# Iterate all preprocessing methods related to feature $D_{*j}$:
	for i in invalid_items:
		# Get the operation number:
		operation_number = i['_id'][0]
		# Get the related invalidated entities id:
		invalidated_entities = i['invalidated_entities']
		# Get the related new entities id:
		new_entities = [e['new_entities'] for e in new_items if e['_id'][0] == operation_number][0]

		# Get the values of the invalidated entities
		invalid_values = entities.aggregate([ \
			{'$match': {'identifier':{'$in':invalidated_entities}}}, \
			{'$project': {'invalid_values': '$attributes.value', '_id': 0}} \
		])
		invalid_values = list(invalid_values)
		invalid_values = [d['invalid_values'] for d in invalid_values]

		# Get the values of the new entities
		new_values = entities.aggregate([ \
			{'$match': {'identifier':{'$in':new_entities}}}, \
			{'$project': {'new_values': '$attributes.value', '_id': 0}} \
		])
		new_values = list(new_values)
		new_values = [d['new_values'] for d in new_values]

		print('------------------------------------------')
		print('Preprocessing methos: ' + operation_number)
		print('Max invalid_values ' + max(invalid_values))
		print('Max new_values ' + max(new_values))

def get_items(feature_name):
	# Get invalidated items for all preprocessing methods related to feature $D_{*j}$:
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
		{'$match': {'activity.attributes.features_name': {'$regex': '.*' + feature_name + '*.'}}}, \
		{'$group': {'_id': '$activity.attributes.operation_number', 'invalidated_entities': {'$addToSet': '$prov:entity'}}} \
	])

	# Get new items for all preprocessing methods related to feature $D_{*j}$:
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
		{'$match': {'activity.attributes.features_name': {'$regex': '.*' + feature_name + '*.'}}}, \
		{'$group': {'_id': '$activity.attributes.operation_number', 'new_entities': {'$addToSet': '$prov:entity'}}} \
	])

	return (invalid_items, new_items)
