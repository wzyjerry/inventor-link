import os
import operator as op
from b_extract_patent import extract

def check_if_personal(inventor_list, company_list):
    companys = set(company_list)
    for inventor in inventor_list:
        if inventor in companys:
            return True
    return False

def fetch_author_org(inventor_list, company_list):
    '''
    return set((author, company)) from AMiner DB
    '''
    pass
    return set()

def fetch_co_author(resolved, unresolved, company_list):
    '''
    return set(((author1, company1), (author2, company2)))
    '''
    pass
    return set()

def direct_link(inventor_list, company_list):
    '''
    return
    status: ['Failed', 'Completed', 'Partial']
    resolved: set((inventor, company))
    unresolved: list(inventor)
    '''
    assert len(inventor_list) > 0
    assert len(company_list) > 0
    resolved = set()
    unresolved = []
    status = 'Failed'
    if len(company_list) == 1:
        for inventor in inventor_list:
            resolved.add((inventor, company_list[0]))
            status = 'Completed'
    else:
        author_org = fetch_author_org(inventor_list, company_list)
        inventor_company = set()
        for inventor in inventor_list:
            for company in company_list:
                inventor_company.add((inventor, company))
        inventor2company = {}
        for pair in author_org & inventor_company:
            inventor2company.setdefault(pair[0], [])
            inventor2company[pair[0]].append(pair[1])
        for inventor in inventor_list:
            if inventor in inventor2company and len(inventor2company[inventor]) == 1:
                resolved.add((inventor, inventor2company[inventor][0]))
            else:
                unresolved.append(inventor)
        if len(unresolved) == 0:
            status = 'Completed'
        elif len(resolved) == 0:
            status = 'Failed'
        else:
            status = 'Partial'
    return status, resolved, unresolved

def link_with_co_author(resolved, unresolved, company_list):
    '''
    return
    resolved: set((inventor, company))
    unresolved: list(inventor)
    '''
    co_inventor = set()
    for pair in resolved:
        for inventor in unresolved:
            for company in company_list:
                t1 = pair
                t2 = (inventor, company)
                if op.gt(t1, t2):
                    t1, t2 = t2, t1
                co_inventor.add((t1, t2))
    co_author = fetch_co_author(resolved, unresolved, company_list)
    inventor2company = {}
    for pair in co_inventor & co_author:
        t = pair[0]
        if t not in resolved:
            t = pair[1]
        inventor2company.setdefault(t[0], [])
        inventor2company[t[0]].append(t[1])
    new_resolved = set()
    new_unresolved = []
    for inventor in unresolved:
        if inventor in inventor2company and len(inventor2company[inventor]) == 1:
            new_resolved.add((inventor, inventor2company[inventor][0]))
        else:
            new_unresolved.append(inventor)
    return new_resolved, new_unresolved


def try_link(inventor_list, company_list):
    '''
    return
    status: ['Failed', 'Completed', 'Partial']
    resolved: set((inventor, company))
    unresolved: list(inventor)
    '''
    # 1. Check if the patent personal
    is_personal = check_if_personal(inventor_list, company_list)
    status = 'Failed'
    resolved = set()
    unresolved = inventor_list
    if not is_personal:
        # 2. Direct link (with (inventor, company))
        status, resolved, unresloved = direct_link(inventor_list, company_list)
        # 3. If partial, try link with co-author
        if status == 'Partial':
            while True:
                new_resolved, new_unresolved = link_with_co_author(resolved, unresloved, company_list)
                if len(new_resolved) == 0:
                    break
                else:
                    resolved = resolved | new_resolved
                    unresloved = new_unresolved
            if len(unresolved) == 0:
                status = 'Completed'
            else:
                status = 'Partial'
        # 4. Future, cluster and allocate
    return status, resolved, unresloved            

if __name__ == "__main__":
    path = './data/EP201901/B1'
    count = 0
    for filename in os.listdir(path):
        patent = extract(os.path.join(path, filename))
        if patent['lang'] == 'en':
            count += 1
    print(count)
