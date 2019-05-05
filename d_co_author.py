import time
import pymongo
from datetime import datetime
from clients.rest import RESTClient

def fetch_db(host):
    return pymongo.MongoClient(host, 9001)['epo_patent']

def fetch_doc(db):
    doc = db['patent_doc']
    return doc

def get_name(inventor):
    name = inventor['name'].split(',')
    name.append(name[0])
    return ' '.join([x.strip() for x in name[1:]])

def gen_coauthor_batch(inventor_list):
    result = []
    name_list = []
    for inventor in inventor_list:
        name_list.append(get_name(inventor))
    size = len(name_list)
    for i in range(size):
        item = []
        for j in range(size):
            if i != j:
                item.append((name_list[j], 1))
        result.append((name_list[i], tuple(item)))
    return tuple(name_list), tuple(result)

if __name__ == "__main__":
    client = RESTClient()
    db = fetch_db('pminer')
    doc = fetch_doc(db)
    count = 0
    for item in doc.find(no_cursor_timeout=True):
        count += 1
        if 'find' in item:
            continue
        print('%s - ID: %d' % (datetime.now(), count))
        inventor_len = len(item['inventor'])
        if 1 < inventor_len <= 10:
            name_list, coauthor_batch = gen_coauthor_batch(item['inventor'])
            while True:
                try:
                    result = client.search_person_by_coauthor_batch(coauthor_batch)
                    break
                except Exception as e:
                    time.sleep(5)
            find = 0
            for name in result:
                if len(name) > 0:
                    find += 1
            if find > 0:
                doc.update_one({'_id': item['_id']}, {
                    '$set': {
                        'find': True,
                        'result': result
                    }
                })
                print('total: %d, find: %d' % (len(name_list), find))
            else:
                doc.update_one({'_id': item['_id']}, {
                    '$set': {
                        'find': False
                    }
                })
            print('')
