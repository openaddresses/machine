import os
from os.path import join, dirname

from pq import PQ
from psycopg2 import connect

def main(DATABASE_URL):
    '''
    '''
    schema_filename = join(dirname(__file__), 'schema.pgsql')

    with connect(DATABASE_URL) as conn:
        with conn.cursor() as db:
            with open(schema_filename) as file:
                db.execute(file.read())
            
            db.execute('DROP TABLE queue')

        pq = PQ(conn)
        pq.create()

if __name__ == '__main__':
    
    DATABASE_URL = os.environ['DATABASE_URL']
    exit(main(DATABASE_URL))
