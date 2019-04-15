import pymongo
from d_get_person_info import fetch_author

def get_collect():
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
    return db[collect_name]

if __name__ == "__main__":
    collect = get_collect()
    count = 0
    num = 0
    for patent in collect.find():
        find = False
        num += 1
        for inventor in patent['inventor']:
            if len(fetch_author(inventor['name'])) > 0:
                find = True
                break
        if find:
            count += 1
        print(count, num)
    print(count)
