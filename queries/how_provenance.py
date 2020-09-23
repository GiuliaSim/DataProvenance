# The inputs and preprocessing methods that created $d_{ij}$
# Input:  $d_{ij}$

import pymongo
import pandas as pd
import pprint
import sys
import time
import random

def get_how_prov(ents_id):
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
		return (input_entities + e, generated_act + a)

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
		entity_id = 'entity:e8d47bb2-65e1-4195-a5da-7f2a5406dff7'

		print('How provenance of: ' + entity_id)

		time1 = time.time()
		# Get the inputs ids and activities id that created an element:
		ents, acts = get_how_prov([entity_id])

		# Find mongodb documents from identifier list:
		why_prov = entities.find({'identifier':{'$in':ents}})
		methods = activities.find({'identifier':{'$in':acts}})

		for m in methods:
			pprint.pprint(m)

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
