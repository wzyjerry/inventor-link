import pymongo

# doc_number\tresult\tfind/total
def fetch_db(host):
    return pymongo.MongoClient(host, 9001)['epo_patent']

def fetch_doc(db):
    doc = db['patent_doc']
    return doc

if __name__ == "__main__":
    db = fetch_db('pminer')
    doc = fetch_doc(db)
    count = 0
    with open('link_result.txt', 'w', encoding='utf-8') as fout:
        for item in doc.find({'find': True}):
            result = item['result']
            count += 1
            find = 0
            for au in result:
                if len(au) > 0:
                    find += 1
            fout.write('%s\t%s\t%d/%d\n' % (item['doc_number'], str(result), find, len(result)))
            fout.flush()
    print(count)
