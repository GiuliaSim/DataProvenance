import pymongo 
import sys
import os
import json


def main(dbname, path):
	client = pymongo.MongoClient('localhost', 27017)
	print('Connected to: localhost')

	dblist = client.list_database_names()
	if dbname in dblist:
	  print('The database exists.')
	else:
		db = client[dbname]

		entities = db['entities']
		activities = db['activities']
		relations = db['relations']

		for folder in os.listdir(path):
			if os.path.isdir(os.path.join(path,folder)):
				for file in os.listdir(os.path.join(path, folder)):
					file_path = os.path.join(path, folder, file)
					if file.startswith('entities') and file.endswith('.json'):
						with open(file_path) as f:
							file_data = json.load(f)
							entities.insert_many(file_data)
							print('Imported entities: ' + file_path)
					if file.startswith('activities') and file.endswith('.json'):
						with open(file_path) as f:
							file_data = json.load(f)
							activities.insert_many(file_data)
							print('Imported activities: ' + file_path)
					if file.startswith('relations') and file.endswith('.json'):
						with open(file_path) as f:
							file_data = json.load(f)
							relations.insert_many(file_data)
							print('Imported relations: ' + file_path)

		client.close()


if __name__ == "__main__":
	if len(sys.argv) == 3 :
		main(sys.argv[1], sys.argv[2])
	else:
		print('[ERROR] usage: create_mongodb.py <db_name> <files_path>')
