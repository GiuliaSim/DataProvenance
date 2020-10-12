# The inputs and preprocessing methods that created $d_{ij}$
# Input:  $d_{ij}$

import pymongo
import pandas as pd
import pprint
import sys
import time
import random

def get_random_item():
	
	# Get output_entities collection
	output_entities = db['output_entities']

	# Get random document on output_entities collection
	random_ent = list(output_entities.aggregate([{'$sample': {'size': 1}}]))
	
	# Get feature_name
	entity_id = random_ent[0]['identifier']

	return entity_id

def get_how_prov2(ents_id):
	# Get input entities from ents_id:
	input_entities = entities.find({'identifier':{'$in': ents_id}, 'attributes.instance':'-1'},{'identifier':1,'_id':0}).distinct('identifier')
	
	# Select intermediate entities:
	diff = lambda l1, l2: [x for x in l1 if x not in l2]
	ents_id = diff(ents_id, input_entities)
	
	# Find the activities that generated the ents_id:
	generated_act = relations.find({'prov:entity': {'$in':ents_id}, 'prov:relation_type':'wasGeneratedBy'}).distinct('prov:activity')

	if not generated_act:
		return (input_entities, generated_act)
	else:
		# Find the entities used by the activities:
		used_ent = relations.find({'prov:activity':{'$in':generated_act}, 'prov:relation_type':'used'}).distinct('prov:entity')

		e, a = get_how_prov(used_ent)

def get_how_prov(ent_id):
	# Get input entity:
	#input_entities = entities.find({'identifier': ent_id, 'attributes.instance':'-1'},{'identifier':1,'_id':0}).distinct('identifier')
	input_entities = entities.aggregate([ \
		{'$match': {'identifier': ent_id, 'attributes.instance':'-1'}}, \
		{'$group': {'_id': '$identifier'}}
	])
	input_entities = [i['_id'] for i in list(input_entities)]


	diff = lambda l1, l2: [x for x in l1 if x not in l2]
	activities = []
	used_ent = []

	# If is not an input entity
	if not input_entities:

		# Find the activities that generated the entity:
		#generated_act = relations.find({'prov:entity': {'$in':ent_id}, 'prov:relation_type':'wasGeneratedBy'},{'prov:activity':1,'_id':0}).distinct('prov:activity')
		generated_act = relations.aggregate([ \
			{'$match': {'prov:entity': {'$in':ent_id}, 'prov:relation_type':'wasGeneratedBy'}}, \
			{'$group': {'_id': '$prov:activity'}}
		])
		generated_act = [i['_id'] for i in list(generated_act)]

		activities = activities + generated_act

		while generated_act:
			#used_ent = relations.find({'prov:activity':{'$in':generated_act}, 'prov:relation_type':'used'},{'prov:entity':1,'_id':0}).distinct('prov:entity')
			used_ent = relations.aggregate([ \
				{'$match': {'prov:activity': {'$in':generated_act}, 'prov:relation_type':'used'}}, \
				{'$group': {'_id': '$prov:entity'}}
			])
			used_ent = [i['_id'] for i in list(used_ent)]

			#generated_act2 = relations.find({'prov:entity': {'$in':used_ent}, 'prov:relation_type':'wasGeneratedBy'},{'prov:activity':1,'_id':0}).distinct('prov:activity')
			generated_act2 = relations.aggregate([ \
				{'$match': {'prov:entity': {'$in':used_ent}, 'prov:relation_type':'wasGeneratedBy'}}, \
				{'$group': {'_id': '$prov:activity'}}
			])
			generated_act2 = [i['_id'] for i in list(generated_act2)]

			generated_act = diff(generated_act2, generated_act)
			activities = activities + generated_act
		return used_ent, activities

	return input_entities, activities

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
		#entity_id = 'entity:e8d47bb2-65e1-4195-a5da-7f2a5406dff7'
		# German
		#entity_id = 'entity:3d2dd83f-d8d0-4f5b-87b7-e8e4a3157c17'
		# Compas
		#entity_id = 'entity:f0c409e5-78f4-496f-bce5-e6ef54b67f60'
		# Census
		#entity_id = 'entity:9b002218-989d-471c-8ba6-494166070157'

		print('How provenance of: ' + entity_id)

		time1 = time.time()
		# Get the inputs ids and activities id that created an element:
		ents, acts = get_how_prov([entity_id])

		# Find mongodb documents from identifier list:
		#why_prov = entities.find({'identifier':{'$in':ents}})
		#methods = activities.find({'identifier':{'$in':acts}})
		print('Number of entities: ' + str(len(ents)))
		print('Number of activities: ' + str(len(acts)))

		time2 = time.time()
		#text = '{:s} function took {:.3f} ms'.format('How Provenance', (time2-time1)*1000.0)
		text = '{:s} function took {:.3f} sec.'.format('How Provenance', (time2-time1))
		print(text)

		# Print description of input entities and preprocessing methods that created the element $d_{ij}$:
		#pprint.pprint(why_prov.explain())
		#pprint.pprint(methods.explain())

		# Close Mongodb connection:
		client.close()
	else:
		print('[ERROR] usage: how_provenance.py <db_name> ')
