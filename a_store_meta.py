import os
import pymongo
import zipfile

def fetch_collect():
    client = pymongo.MongoClient('pminer', 9001)
    collect = client['epo_patent']['patent_meta']
    collect.create_index([('filename', pymongo.ASCENDING)], unique=True)
    return collect

if __name__ == "__main__":
    topPath = input("Path: ")
    collect = fetch_collect()
    count = 0
    docs = []
    for path, dirs, files in os.walk(topPath):
        for name in files:
            if '.zip' in name:
                filename = os.path.join(path, name)
                fzip = zipfile.ZipFile(filename)
                for zipName in fzip.namelist():
                    if 'TOC' not in zipName and '.xml' in zipName:
                        xml = fzip.read(zipName)
                        docs.append({
                            'xml': str(xml, encoding='utf-8'),
                            'filename': zipName
                        })
                        count += 1
                        if count % 1000 == 0:
                            try:
                                collect.insert_many(docs, ordered=False)
                            except Exception as e:
                                print(e)
                            docs = []
                            print(count)
    try:
        collect.insert_many(docs, ordered=False)
    except Exception as e:
        print(e)
    print(count)
