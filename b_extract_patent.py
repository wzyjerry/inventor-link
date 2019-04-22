import json
import pymongo
import xmltodict

def fetch_db(host):
    return pymongo.MongoClient(host, 9001)['epo_patent']

def fetch_meta(db):
    return db['patent_meta']

def fetch_doc(db):
    doc = db['patent_doc']
    doc.create_index([('doc_number', pymongo.ASCENDING)], unique=True)
    return doc

def fetch_inventor(db):
    inventor = db['patent_inventor']
    inventor.create_index([
        ('name', pymongo.ASCENDING),
        ('addr.str', pymongo.ASCENDING),
        ('addr.city', pymongo.ASCENDING),
        ('addr.ctry', pymongo.ASCENDING)
    ], unique=True)
    inventor.create_index([('name', pymongo.ASCENDING)])
    inventor.create_index([('addr.ctry', pymongo.ASCENDING)])
    return inventor

def fetch_company(db):
    company = db['patent_company']
    company.create_index([
        ('name', pymongo.ASCENDING),
        ('addr.str', pymongo.ASCENDING),
        ('addr.city', pymongo.ASCENDING),
        ('addr.ctry', pymongo.ASCENDING)
    ], unique=True)
    company.create_index([('name', pymongo.ASCENDING)])
    company.create_index([('addr.ctry', pymongo.ASCENDING)])
    return company

def resolve_ipcr(field_ipcr):
    value = set()
    if isinstance(field_ipcr, list):
        for item in field_ipcr:
            value.add(' '.join(item['text'].split()[:2]))
    else:
        value.add(' '.join(field_ipcr['text'].split()[:2]))
    return value

def resolve_title(field_title):
    value = {}
    if 'B541' in field_title:
        for lang, title in zip(field_title.get('B541'), field_title.get('B542')):
            value[lang] = title
    return value

def resolve_name(field_name):
    if 'B725EP' in field_name:
        return False, None
    value = {
        'name': '',
        'addr': {
            'str': '',
            'city': '',
            'ctry': ''
        }
    }
    if 'snm' in field_name:
        value['name'] = field_name.get('snm')
    if 'adr' in field_name:
        adr = field_name.get('adr')
        if 'str' in adr:
            value['addr']['str'] = adr.get('str')
        if 'city' in adr:
            value['addr']['city'] = adr.get('city')
        if 'ctry' in adr:
            value['addr']['ctry'] = adr.get('ctry')
    return True, value

def resolve_inventor(field_inventor):
    value = []
    B721 = field_inventor.get('B721')
    if isinstance(B721, list):
        for item in B721:
            valid, name = resolve_name(item)
            if valid:
                value.append(name)
    else:
        valid, name = resolve_name(B721)
        if valid:
            value.append(name)
    return value

def resolve_applicant(field_applicant):
    value = []
    B711 = field_applicant.get('B711')
    if isinstance(B711, list):
        for item in B711:
            valid, name = resolve_name(item)
            if valid:
                value.append(name)
    else:
        valid, name = resolve_name(B711)
        if valid:
            value.append(name)
    return value

def resolve_grantee(field_grantee):
    value = []
    B731 = field_grantee.get('B731')
    if isinstance(B731, list):
        for item in B731:
            valid, name = resolve_name(item)
            if valid:
                value.append(name)
    else:
        valid, name = resolve_name(B731)
        if valid:
            value.append(name)
    return value

def extract(meta_doc):
    patent = {
        'doc_number': '',
        'dates': {},
        'lang': 'xx',
        'ipcr': [],
        'title': {},
        'inventor': [],
        'company': [],
        'link': [meta_doc['filename']]
    }
    json = xmltodict.parse(meta_doc['xml'])
    root = json.get('ep-patent-document')
    SDOBI = root.get('SDOBI')

    # B100 (B110, B130, B140, B190)
    B100 = SDOBI.get('B100')
    # B110 doc_number
    B110 = B100.get('B110')
    patent['doc_number'] = B110
    # B130 kind (A1, A2, A3, A8, A9, B1, B2, B8, B9)
    B130 = B100.get('B130')
    # B140 (date) publication date
    B140 = B100.get('B140').get('date')
    patent['dates'][B130] = {
        'publication': int(B140)
    }

    if 'B200' in SDOBI:
        # B200 (B210, B220)
        B200 = SDOBI.get('B200')
        B220 = B200.get('B220').get('date')
        patent['dates'][B130]['application'] = int(B220)
        if 'B260' in B200:
            # B260 lang
            B260 = B200.get('B260')
            patent['lang'] = B260
    
    if 'B500' in SDOBI:
        # B500 TECHNICAL INFORMATION
        B500 = SDOBI.get('B500')
        if 'B510EP' in B500:
            # B510EP (classification-ipcr+)
            B510EP = B500.get('B510EP')
            classification_ipcr = B510EP.get('classification-ipcr')
            patent['ipcr'].extend(resolve_ipcr(classification_ipcr))
        if 'B540' in B500:
            # B540 (B541?, B542, B542EP?)+ Title
            B540 = B500.get('B540')
            patent['title'].update(resolve_title(B540))

    if 'B700' in SDOBI:
        # B700 PARTIES
        B700 = SDOBI.get('B700')
        if 'B710' in B700:
            # B710 (B711+) Applicants
            B710 = B700.get('B710')
            patent['company'].extend(resolve_applicant(B710))
        if 'B720' in B700:
            # B720 (B721+, B721EP?) Inventors
            B720 = B700.get('B720')
            patent['inventor'].extend(resolve_inventor(B720))
        if 'B730' in B700:
            # B730 (B731+) Grantees
            B730 = B700.get('B730')
            patent['company'].extend(resolve_grantee(B730))
    return patent

def gen_id(patent, inventor, company):
    for item in patent['inventor']:
        log = inventor.find_one(item)
        if log is None:
            inventor.insert_one(item)
        else:
            item['_id'] = log['_id']
    for item in patent['company']:
        log = company.find_one(item)
        if log is None:
            company.insert_one(item)
        else:
            item['_id'] = log['_id']

def merge(old, new):
    old['dates'].update(new['dates'])
    if new['lang'] != 'xx':
        old['lang'] = new['lang']
    old['title'].update(new['title'])
    old['ipcr'] = list(set(old['ipcr']) | set(new['ipcr']))
    old['link'] = list(set(old['link']) | set(new['link']))
    inventor = {item['_id']: item for item in old['inventor']}
    for item in new['inventor']:
        inventor[item['_id']] = item
    old['inventor'] = list(inventor.values())
    company = {item['_id']: item for item in old['company']}
    for item in new['company']:
        company[item['_id']] = item
    old['company'] = list(company.values())

if __name__ == "__main__":
    db = fetch_db('localhost')
    meta = fetch_meta(db)
    inventor = fetch_inventor(db)
    company = fetch_company(db)
    doc = fetch_doc(db)
    count = 0
    for meta_doc in meta.find():
        patent = extract(meta_doc)
        gen_id(patent, inventor, company)
        old = doc.find_one({'doc_number': patent['doc_number']})
        if old is not None:
            merge(old, patent)
            doc.replace_one({'_id': old['_id']}, old)
        else:
            doc.insert_one(patent)
        count += 1
        if count % 1000 == 0:
            print(count)
    print(count)
    print('End.')
