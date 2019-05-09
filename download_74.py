import os
import subprocess
from bs4 import BeautifulSoup

def gen_zip_link(filename, prefix):
    link = {}
    with open(filename, 'r', encoding='utf-8') as fin:
        html = fin.read()
    soup = BeautifulSoup(html, 'html.parser')
    for a in soup.find_all('a', href=True):
        suffix = a['href']
        if suffix.endswith('.zip'):
            filename = suffix[suffix.rfind('/') + 1:]
            link.setdefault(filename, {})
            if suffix.startswith('download'):
                link[filename]['download'] = (prefix + suffix, filename)
            elif suffix.startswith('checksum'):
                link[filename]['checksum'] = (prefix + suffix, filename + '.sha1')
    return link

def check_sha1(filename):
    print('check %s' % filename)
    with open(filename + '.sha1', 'r', encoding='utf-8') as fin:
        sha1 = fin.read().strip().lower()
        print('sha1: %s' % sha1)
    file_sha1 = subprocess.getoutput('sha1sum %s' % filename).split()[0]
    print('file_sha1: %s' % file_sha1)
    if sha1 == file_sha1:
        return True
    return False

def auth():
    cmd = 'wget --post-data="action=1&login=jas2015mine@163.com&pwd=3XbMA.ZWBnKd&submit=Log in" --save-cookies=cookie --keep-session-cookies https://publication.epo.org/raw-data/authentication -O authentication'
    subprocess.call(cmd, shell=True)

def try_download(url, folder):
    print('Download %s' % url[0])
    while True:
        cmd = 'wget --load-cookies=cookie --keep-session-cookies %s -O %s/%s' % (url[0], folder, url[1])
        subprocess.call(cmd, shell=True)
        if os.path.getsize('%s/%s' % (folder, url[1])) == 215:
            auth()
        else:
            break

if __name__ == "__main__":
    page = '74.html'
    prefix = 'https://publication.epo.org/raw-data/'
    link = gen_zip_link(page, prefix)
    folder = '74'
    for filename in link:
        if 'download' in link[filename]:
            try_download(link[filename]['checksum'], folder)
            while True:
                try_download(link[filename]['download'], folder)
                if check_sha1('%s/%s' % (folder, filename)):
                    break
