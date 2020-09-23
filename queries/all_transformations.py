# Set of preprocessing methods applied to $D$, and the features, $D_{*j}$, they affect.
# Input: D - dataframe

import pymongo
import pandas as pd
import sys
import time


def all_transformations(db):
	pipeline = [
		{
			'$group': {
				'_id':  '$attributes.function_name',
				'features_name': {'$addToSet':'$attributes.features_name'}
			}
		}

	]

	#all_act = db.command('aggregate','activities',pipeline=pipeline,explain=True)
	all_act = db.command('explain',{'aggregate':'activities','pipeline':pipeline,'cursor':{}}, verbosity='executionStats')

	return all_act

if __name__ == "__main__":

	if len(sys.argv) == 2 :
		dbname = sys.argv[1]

		# Connect with MongoClient on the default host and port:
		client = pymongo.MongoClient('localhost', 27017)
		
		# Getting a Database:
		db = client[dbname]
		#activities = db.activities

		
		time1 = time.time()
		all_act = all_transformations(db)
		time2 = time.time()

		print('executionTimeMillis:')
		print(all_act['stages'][0]['$cursor']['executionStats']['executionTimeMillis'])


		# Print result:
		#for doc in all_act:
		#	print(doc)


		text = '{:s} function took {:.3f} sec.'.format('All Transformations', (time2-time1))
		print(text)

		# Close Mongodb connection:
		client.close()

	else:
		print('[ERROR] usage: create_mongodb.py <db_name> <files_path>')

