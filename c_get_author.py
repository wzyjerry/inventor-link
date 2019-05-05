import time
import pymongo
from clients.rest import RESTClient

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

def resolve_name(field_name):
    name = field_name['name']
    addr = field_name['addr']
    ctry = addr['ctry']
    name_split = [x.strip() for x in name.split(',')]
    if ctry == '':
        return False, ctry
    elif ctry in ['CN', 'TW']:
        return True, ' '.join(name_split)
    else:
        return False, ctry

def get_author(client, inventor):
    name = inventor['name'].split(',')
    name.append(name[0])
    name = ' '.join([x.strip() for x in name[1:]])
    while True:
        try:
            result = client.search_person_advanced(name=name, size=1)
            break
        except Exception as e:
            time.sleep(5)
    total = result['total']
    find = False
    total_item = []
    if total > 0:
        find = True
        offset = 0
        size = total
        while size > 0:
            item = client.search_person_advanced(name=name, offset=offset, size=size)['result']
            length = len(item)
            if length == 0:
                break
            offset += length
            size -= length
            total_item.extend(item)
        print(total, len(total_item))
    return find, name, total_item

if __name__ == "__main__":
    client = RESTClient()
    db = fetch_db('localhost')
    inventor = fetch_inventor(db)
    author = fetch_author(db)
    count = 0
    for item in inventor.find(no_cursor_timeout=True):
        count += 1
        if count % 1000 == 0:
            print(count)
        if author.find_one({'iid': item['_id']}) is None:
            find, name, alist = get_author(client, item)
            doc = {
                'iid': item['_id'],
                'name': name,
                'find': find
            }
            if find:
                doc['author'] = alist
            author.insert_one(doc)
    