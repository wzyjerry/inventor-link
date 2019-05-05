import pymongo
from clients.rest import RESTClient

client = RESTClient()

def fetch_db(host):
    return pymongo.MongoClient(host, 9001)['epo_patent']

def fetch_author(db):
    author = db['aminer_author']
    author.create_index([('iid', pymongo.ASCENDING)], unique=True)
    author.create_index([('name', pymongo.ASCENDING)])
    return author

def fetch_inventor(db):
    inventor = db['patent_inventor']
    return inventor

def fetch_doc(db):
    doc = db['patent_doc']
    return doc
# print(client.search_person('Ben Han', offset=0))

if __name__ == "__main__":
    db = fetch_db('pminer')
    doc = fetch_doc(db)
    total = doc.count({
        'find': {
            '$exists': True
        }
    })
    find = doc.count({
        'find': True
    })
    print('find/total: %d/%d' % (find, total))
    # print(list(inventor.aggregate([
    #     {'$group': {'_id': '$name', 'count': {'$sum': 1}}},
    #     {'$match': {'count': {'$gt': 1}}}
    # ])))
