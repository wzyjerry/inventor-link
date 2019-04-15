import os
import xmltodict

def extract(filename):
    def resolve_ipc(ipc):
        return ' '.join(ipc.split()[:2])
    def resolve_name(name):
        if name.get('B725EP'):
            return {
                'name': name.get('B725EP').get('text')
            }
        result = {
            'name': name.get('snm'),
            'adr': {}
        }
        if name.get('iid'):
            result['iid'] = name.get('iid')
        if name.get('irf'):
            result['irf'] = name.get('irf')
        adr = name.get('adr')
        if adr.get('str'):
            result['adr']['str'] = adr.get('str')
        if adr.get('city'):
            result['adr']['city'] = adr.get('city')
        if adr.get('ctry'):
            result['adr']['ctry'] = adr.get('ctry')
        return result
        
    patent = {}
    with open(filename, 'r', encoding='utf-8') as fin:
        xml = fin.read()
        json = xmltodict.parse(xml)
        root = json.get('ep-patent-document')
        SDOBI = root.get('SDOBI')
        B100 = SDOBI.get('B100')
        patent['source'] = B100.get('B190')
        patent['kind'] = B100.get('B130')
        patent['patent_number'] = patent['source'] + B100.get('B110') + patent['kind']
        patent['issue_date'] = B100.get('B140').get('date')
        B200 = SDOBI.get('B200')
        patent['application_number'] = B200.get('B210')
        patent['application_date'] = B200.get('B220').get('date')
        patent['lang'] = B200.get('B260')
        B500 = SDOBI.get('B500')
        patent['ipcr'] = []
        ipcr = B500.get('B510EP').get('classification-ipcr')
        if type(ipcr) is list:
            for ipc in ipcr:
                patent['ipcr'].append(resolve_ipc(ipc.get('text')))
        else:
            patent['ipcr'].append(resolve_ipc(ipcr.get('text')))
        patent['title'] = {}
        B540 = B500.get('B540')
        for title in zip(B540.get('B541'), B540.get('B542')):
            patent['title'][title[0]] = title[1]
        B700 = SDOBI.get('B700')
        B720 = B700.get('B720')
        patent['inventor'] = []
        if type(B720.get('B721')) is list:
            for inventor in B720.get('B721'):
                patent['inventor'].append(resolve_name(inventor))
        else:
            patent['inventor'].append(resolve_name(B720.get('B721')))
        B730 = B700.get('B730')
        patent['company'] = []
        if type(B730.get('B731')) is list:
            for company in B730.get('B731'):
                patent['company'].append(resolve_name(company))
        else:
            patent['company'].append(resolve_name(B730.get('B731')))
    return patent


if __name__ == "__main__":
    patent = extract('data/EP201901/B1/EP00901658NWB1.xml')
    print(patent)
    patent = extract('data/EP201901/B1/EP01300569NWB1.xml')
    print(patent)
