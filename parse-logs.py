from sys import argv
from re import compile, match
from datetime import datetime
from itertools import chain
from os.path import basename

filename = argv[1]

with open(filename) as file:
    '''
     MainThread  2014-10-30 05:23:01,042   INFO: Loaded 4 sources from state.txt
       Thread-2  2014-10-30 05:23:01,045   INFO: /var/opt/openaddresses/sources/us-ma-melrose.json
    '''
    pattern = compile(r'^ *(\w*Thread)(-(?P<thread>\d+))? +(?P<timestamp>20\S+ \S+) +(?P<loglevel>\w+): (?P<message>.+)$')
    matches = [(index, pattern.match(line)) for (index, line) in enumerate(file)]
    entries = [(int(m.group('thread') or 0),
                
                # they're not really microseconds (%f) but good enough to sort
                datetime.strptime(m.group('timestamp'), '%Y-%m-%d %H:%M:%S,%f'),
                
                i, m.group('message'), m.group('loglevel'))

               for (i, m) in matches if m]

log = iter(sorted(entries))

#
# Burn through the main thread
#
for (thread, timestamp, lineno, message, loglevel) in log:
    
    if thread > 0:
        log = chain([(thread, timestamp, lineno, message, loglevel)], log)
        break

#
# Look at all the cache tasks
#
while True:
    thread, timestamp, lineno, message, loglevel = log.next()
    
    if match(r'^\d+ source files remain', message):
        # Stop as soon as we see one of these timer thread messages,
        # they come after the main block of worker thread messages.
        break

    if not (match(r'^\S+.json$', message) and loglevel == 'INFO'):
        # Move on to the conform tasks if we don't see a cache source.
        log = chain([(thread, timestamp, lineno, message, loglevel)], log)
        break

    source = basename(message)
    thread, timestamp, lineno, message, loglevel = log.next()
    
    if not (match(r'^openaddresses-cache \S+ \S+/cache-\S+$', message) and loglevel == 'DEBUG'):
        # Fail if we don't see debug output from the cache subprocess.
        raise ValueError(thread, timestamp, lineno, message, loglevel)

    start_cache = timestamp
    thread, timestamp, lineno, message, loglevel = log.next()

    if not (match(r'^(\S+) --> \S+/cache-\S+$', message) and loglevel == 'DEBUG'):
        # Fail if we don't see debug output from after the cache subprocess.
        raise ValueError(thread, timestamp, lineno, message, loglevel)

    end_cache = timestamp
    cache_time = end_cache - start_cache
    
    print source, 'cache', cache_time

#
# Look at all the conform tasks
#
while True:
    thread, timestamp, lineno, message, loglevel = log.next()

    if match(r'^\d+ source files remain', message):
        # Ignore timer thread messages.
        continue

    if not (match(r'^\S+.json$', message) and loglevel == 'INFO'):
        # Finish up if we don't see a conform source.
        log = chain([(thread, timestamp, lineno, message, loglevel)], log)
        break

    source = basename(message)
    thread, timestamp, lineno, message, loglevel = log.next()
    
    if not (match(r'^openaddresses-conform \S+ \S+/conform-\S+$', message) and loglevel == 'DEBUG'):
        # Fail if we don't see debug output from the conform subprocess.
        raise ValueError(thread, timestamp, lineno, message, loglevel)

    start_conform = timestamp
    thread, timestamp, lineno, message, loglevel = log.next()

    if not (match(r'^(\S+) --> \S+/conform-\S+$', message) and loglevel == 'DEBUG'):
        # Move along if we don't see debug output from after the conform subprocess.
        log = chain([(thread, timestamp, lineno, message, loglevel)], log)
        continue

        # Fail if we don't see debug output from after the conform subprocess.
        raise ValueError(thread, timestamp, lineno, message, loglevel)

    end_conform = timestamp
    conform_time = end_conform - start_conform
    
    print source, 'conform', conform_time
    
    for (thread, timestamp, lineno, message, loglevel) in log:
        if not match(r'^out/\S+$', message):
            log = chain([(thread, timestamp, lineno, message, loglevel)], log)
            break
