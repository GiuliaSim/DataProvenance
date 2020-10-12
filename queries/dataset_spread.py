# For all preprocessing methods on $D$, what is the change in dataset spread.
# Input: D - dataframe

import pymongo
import pandas as pd
import pprint
import time
import sys

def get_items():
	# Get the feature name of the output_entities
	output_features = output_entities.aggregate([ \
		{'$group': {'_id': '$attributes.feature_name'}}
	],allowDiskUse=True)
	output_features = [i['_id'] for i in output_features]

	# Get the indexes of the output_entities
	output_indexes = output_entities.aggregate([ \
		{'$group': {'_id': '$attributes.index'}}
	],allowDiskUse=True)
	output_indexes = [i['_id'] for i in output_indexes]

	invalid_items = entities.aggregate([
		{'$match': {'$or': [
			{'attributes.feature_name': { '$nin': output_features }}, 
			{'attributes.index': { '$nin': output_indexes }}
		]}},
		{'$group': {
			'_id': {'feature_name':'$attributes.feature_name', 'index':'$attributes.index'},
			'maxInstance': {'$max':'$attributes.instance'},
			#'identifier': {'$first': '$identifier'}
			'entity': {'$push':'$$ROOT'}
		}}
	],allowDiskUse = True)

	new_items = entities.aggregate([
		{'$group': {
			'_id': {'feature_name':'$attributes.feature_name', 'index':'$attributes.index'},
			'maxInstance': {'$max':'$attributes.instance'},
			#'identifier': {'$first': '$identifier'}
			'entity': {'$push':'$$ROOT'}
		}},
		{'$match': {'maxInstance': { '$ne': '-1' }}}
	],allowDiskUse = True)

	invalid_items = [i['entity'] for i in invalid_items]
	new_items = [i['entity'] for i in new_items]

	#pprint.pprint(invalid_items)
	#pprint.pprint(new_items)

	return (invalid_items,new_items)

def get_features():
	diff = lambda l1, l2: [x for x in l1 if x not in l2]

	# Get the feature name of the output_entities
	output_features = output_entities.aggregate([ \
		{'$group': {'_id': '$attributes.feature_name', 'ids': {'$sum': 1}}}
	],allowDiskUse=True)
	output_features = [i['_id'] for i in output_features]
	print('Number of output features: ' + str(len(output_features)))

	# Get the feature_name of the input dataset
	input_features = entities.aggregate([
		{'$match': {'attributes.instance': '-1'}}, \
		{'$group': {'_id': '$attributes.feature_name'}}
	])
	input_features = [i['_id'] for i in input_features]
	print('Number of input features: ' + str(len(input_features)))

	# Get the feature name of the new entities
	new_features = diff(output_features, input_features)

	# Get all feature_name of the dataset
	all_features = entities.aggregate([
		{'$group': {'_id': '$attributes.feature_name'}}
	])
	all_features = [i['_id'] for i in all_features]

	# Get the feature name of the invalidated entities
	invalid_features = diff(all_features, output_features)

	return (invalid_features, new_features)

def get_method_number(activities_byON, method_id):
	for a in activities_byON:
		method_number = a['_id']
		activities_id = a['activities']
		if method_id in activities_id:
			return method_number

	return None


def get_dataset_spread():
	activities_byON = activities.aggregate([
		{'$group': {
			'_id': '$attributes.operation_number',
			'activities': {'$addToSet': '$identifier'}
		}},
	])
	activities_byON = list(activities_byON)

	generated_methods = entities.aggregate([
		{'$match': {'attributes.instance': {'$ne':'-1'}}},
		{'$group': {
			'_id': '$attributes.instance',
			'entities': {'$sum': 1}
			#'entities': {'$addToSet': '$identifier'}
		}},
		#{'$project':{'operation_number':'$_id','entities':'$entities'}},
		#{'$out':'preprocessing_method'}
	],allowDiskUse = True)
	generated_methods = list(generated_methods)

	invalidated_methods = relations.aggregate([
		{'$match': {'prov:relation_type': 'wasInvalidatedBy'}},
		{'$group': {
			'_id': '$prov:activity',
			'entities': {'$sum': 1}
			#'entities': {'$addToSet': '$prov:entity'}
		}},
	])

	methods = {}

	for method in generated_methods:
		method_number = method['_id']
		activities_id = activities.find({'attributes.operation_number': method_number},{'identifier':1,'_id':0}).distinct('identifier')
		new_entities = method['entities']
		methods[method_number] = {'activities': len(activities_id)}
		methods[method_number] = {'new_entities':new_entities}

	for method in invalidated_methods:
		method_id = method['_id']
		method_number = get_method_number(activities_byON, method_id)
		invalidated_entities = method['entities']
		if method_number in methods:
			if 'invalidated_entities' in methods[method_number]:
				number = methods[method_number]['invalidated_entities'] + invalidated_entities
			else:
				number = invalidated_entities
			methods[method_number]= {'invalidated_entities':number}
			if 'activities' in methods[method_number]:
				methods[method_number]['activities'] += 1
			else:
				methods[method_number]['activities'] = 1
		else:
			methods[method_number]= {'invalidated_entities':invalidated_entities}


	print('Number of preprocessing methods: ' + str(len(methods)))

	for method_number, method in methods.items():
		print('Preprocessing method number ' + str(method_number) + ' :')
		if 'activities' in method:
			print(str(method['activities']) + ' activities')
		if 'new_entities' in method:
			print(str(method['new_entities']) + ' new entities')
		if 'invalidated_entities' in method:
			print(str(method['invalidated_entities']) + ' invalidated entities')

	#if invalid_items:
	#	print('Max Feature name on invlidated entities: ' + max(invalid_feature))
	#if new_items:
	#	print('Max Feature name on new entities: ' + max(new_feature))


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
		invalid_features, new_features = get_features()
		print ('Number of invalidated features: ' + str(len(invalid_features)))
		print ('Number of new features: ' + str(len(new_features)))

		invalid_items, new_items = get_items()
		print('Number of invalidated items: ' + str(len(invalid_items)))
		print('Number of new items: ' + str(len(new_items)))

		get_dataset_spread()
		time2 = time.time()

		text = '{:s} function took {:.3f} sec.'.format('Dataset Spread', (time2-time1))
		print(text)
		
		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: dataset_spread.py <db_name>')
