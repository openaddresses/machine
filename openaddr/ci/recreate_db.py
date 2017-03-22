import os
from os.path import join, dirname

from pq import PQ
from psycopg2 import connect

def recreate(DATABASE_URL):
    '''
    '''
    ci_schema_filename = join(dirname(__file__), 'schema.pgsql')
    cov_schema_filename = join(dirname(__file__), 'coverage', 'schema.pgsql')

    with connect(DATABASE_URL) as conn:
        with conn.cursor() as db:
            db.execute('SET client_min_messages TO WARNING')
        
            with open(ci_schema_filename) as file:
                db.execute(file.read())
            
            with open(cov_schema_filename) as file:
                db.execute(file.read())
            
            db.execute('DROP TABLE IF EXISTS queue')

        pq = PQ(conn, table='queue')
        pq.create()

def main():
    '''
    '''
    DATABASE_URL = os.environ['DATABASE_URL']
    return recreate(DATABASE_URL)

if __name__ == '__main__':
    exit(main())
