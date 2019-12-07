import json
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['FreewayDB']
collection_freeway = db['write_collection_3']

with open('oneHour.json') as f:
    file_data = json.load(f)

# if pymongo < 3.0, use insert()
collection_freeway.insert(file_data)
# if pymongo >= 3.0 use insert_one() for inserting one document
#collection_freeway.insert_one(file_data)
# if pymongo >= 3.0 use insert_many() for inserting many documents
collection_freeway.posts.insert_many(file_data)
client.close()