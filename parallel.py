from threading import Thread, Lock
from logging import getLogger, StreamHandler, Formatter, DEBUG
from multiprocessing import cpu_count
from openaddr import conform

def run_thread(lock, source_files):
    '''
    '''
    while True:
        with lock:
            if not source_files:
                break
            path = source_files.pop()
            getLogger('openaddr').info(path)
    
        conform(path, 'out')

handler = StreamHandler()
handler.setFormatter(Formatter('%(threadName)10s %(relativeCreated)10.1f %(levelname)06s: %(message)s'))
getLogger('openaddr').addHandler(handler)
getLogger('openaddr').setLevel(DEBUG)

source_files = [
    '/var/opt/openaddresses/sources/us-ca-san_francisco.json',
    '/var/opt/openaddresses/sources/us-ca-oakland.json',
    '/var/opt/openaddresses/sources/us-ca-berkeley.json'
    ]

lock = Lock()

threads = [Thread(target=run_thread, args=(lock, source_files))
           for i in range(cpu_count() + 1)]

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()
