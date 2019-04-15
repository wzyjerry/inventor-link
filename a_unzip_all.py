import os
import zipfile

def unzip(path, to):
    for filename in os.listdir(path):
        filename = os.path.join(path, filename)
        fzip = zipfile.ZipFile(filename)
        for name in fzip.namelist():
            if 'TOC' not in name and '.xml' in name:
                fzip.extract(name, to)

if __name__ == "__main__":
    unzip('./data/EP201901/DOC/EPNWB1', './data/EP201901/B1')
