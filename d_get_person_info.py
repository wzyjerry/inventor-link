import json
import requests

def fetch_author(name):
    result = requests.get('http://api.aminer.org/api/search/person/advanced', params={'name': name}, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'
    }).json()['result']
    author_list = []
    for author in result:
        if name.lower() == author['name'].lower() and 'desc' in author['aff']:
            author_list.append({
                'name': author['name'],
                'org': author['aff']['desc']
            })
    return author_list

if __name__ == "__main__":
    author_list = fetch_author('jie tang')
    print(author_list)
