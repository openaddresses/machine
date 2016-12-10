#!/usr/bin/env python3

"Python script to pull stats about OpenAddresses runs from the database and convert to JSON"

import json, os, sys
from .. import util

def make_stats(cur):
    "Connect to the database and extract stats data, transforming into a JSON-friendly object"

    ### JSON output data object
    data = {
        'timeseries': [],
        'last_address_counts': [],
        'last_process_times': [],
        'last_cache_times': [],
        'lost_sources': { 'headers': ['Source', 'Max Addresses', 'Last Good'],
                          'rows': []},
    }

    ### Get the timestamp of the last run
    cur.execute('select max(tsName) from dashboard_stats')
    last_ts = cur.fetchone()[0]

    ### data['timeseries']: summary statistics for all runs
    cur.execute('''
        select tsName,
               sum(address_count) as addresses,
               count(*) as successful_sources,
               avg(cache_time) as average_cache_time,
               avg(process_time) as average_process_time
        from dashboard_stats
        where address_count > 0
        group by tsName
        order by tsName;
    ''')
    for row in cur.fetchall():
        data['timeseries'].append({
            'ts': int(float(row[0])*1000),
            'addresses': row[1],
            'successful_sources': row[2],
            'average_cache_time': row[3],
            'average_process_time': row[4]
            })


    ### data['last_address_counts'] and friends: detailed performance stats for the last run
    cur.execute('''
        select address_count, cache_time, process_time
        from dashboard_stats
        where tsName = %s
    ''', (last_ts,))
    results = cur.fetchall()
    address_counts, cache_times, process_times = zip(*results)
    def strip_and_sort(d):
        return sorted((x for x in d if x is not None))
    data['last_address_counts'] = strip_and_sort(address_counts)
    data['last_process_times'] = strip_and_sort(process_times)
    data['last_cache_times'] = strip_and_sort(cache_times)


    ### data['lost_sources']: sources which previously had addresses, but not in the last run

    # First calculate a list of all sources that did work once, but didn't last run
    # Could probably use a SQL subselect for this, but let's just do it in Python
    cur.execute('''
        select source
        from dashboard_stats
        where tsName = %s
              and (address_count = 0 or address_count is null);
    ''', (last_ts,))
    r = cur.fetchall()
    no_address_list = [row[0] for row in r]

    # Now get details from the last successful run of each of those failed sources
    cur.execute('''
        select source, max(address_count) as ac, max(tsName)
        from dashboard_stats
        where source = any(%s)
              and address_count > 0
        group by source
        order by ac desc
    ''', (no_address_list,))
    for source, address_count, ts in cur.fetchall():
        data['lost_sources']['rows'].append((source, address_count, ts))

    return data

def upload_stats(s3, data):
    stats_key = s3.new_key('machine-stats.json')
    stats_key.set_contents_from_string(json.dumps(data),
        policy='public-read', headers={'Content-Type': 'application/json'})

    return util.s3_key_url(stats_key)

if __name__ == '__main__':
    data = make_stats(sys.argv[1])
    sys.stdout.write(json.dumps(data))
