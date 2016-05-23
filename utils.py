import requests
import os
import zipfile


def fetch(url, filepath):
    r = requests.get(url, stream=True)
    with open(filepath, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()

    return filepath


def unzip(filepath, dest):
    with zipfile.ZipFile(filepath) as zf:
        zf.extractall(dest)


def rlistdir(path):
    files = []
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            files.append(os.path.join(dirpath, f))

    return files
