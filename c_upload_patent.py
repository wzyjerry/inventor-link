import os
import pymongo
from b_extract_patent import extract

host = '210.45.215.233'
port = 9001
user = 'admin'
pswd = 'admin'
authdb = 'admin'
db_name = 'epo_patent'
collect_name = 'en_b1'

client = pymongo.MongoClient(host, port)
auth = client[authdb]
auth.authenticate(user, pswd)
db = client[db_name]
collect = db[collect_name]
collect.create_index([('patent_number', pymongo.ASCENDING)], unique=True)

path = './data/EP201901/B1'

docs = []
for filename in os.listdir(path):
    patent = extract(os.path.join(path, filename))
    if patent['lang'] == 'en':
        docs.append(patent)

try:
    result = collect.insert_many(docs, ordered=False)
except Exception as e:
    print(e)
