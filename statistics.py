import json
import pymongo

def fetch_db(host):
    return pymongo.MongoClient(host, 9001)['epo_patent']

def fetch_doc(db):
    doc = db['patent_doc']
    return doc

if __name__ == "__main__":
    db = fetch_db('pminer')
    doc = fetch_doc(db)
    stat = {
        'total': 0,
        'find': 0,
        'accuracy': 0.0,
        'inventor': 0,
        'match': 0,
        'average match': 0.0,
        'distinct author': 0,
        'max': [('id', 0, set())]
    }
    author = {}
    for item in doc.find():
        stat['total'] += 1
        if 'find' in item and item['find'] == True:
            stat['find'] += 1
            result = item['result']
            find = 0
            for au in result:
                if len(au) > 0:
                    find += 1
                    author.setdefault(au[0]['ID'], set())
                    author[au[0]['ID']].add((item['doc_number'], item['title']['en']))
            stat['inventor'] += len(item['inventor'])
            stat['match'] += find
    stat['accuracy'] = stat['find'] / stat['total']
    stat['average match'] = stat['match'] / stat['inventor']
    stat['distinct author'] = len(author)
    m = 0
    result = []
    for au in author:
        if len(author[au]) > m:
            m = len(author[au])
            result = [(au, m, list(author[au]))]
        elif len(author[au]) == m:
            result.append((au, m, list(author[au])))
    stat['max'] = result
    with open('statistics.txt', 'w', encoding='utf-8') as fout:
        fout.write(json.dumps(stat, ensure_ascii=False, indent=2))
