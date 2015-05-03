# TODO: factor out of app.py

from psycopg2 import connect
from pq import PQ

# TODO factor this cleanly out of app.py

TASK_QUEUE, DONE_QUEUE = 'tasks', 'finished'
MAGIC_OK_MESSAGE = 'Everything is fine'

def db_connect(dsn):
    ''' Connect to database using Flask app instance or DSN string.
    '''
    return connect(dsn)

def db_queue(conn, name=None):
    return PQ(conn, table='queue')[name or TASK_QUEUE]

def db_cursor(conn):
    return conn.cursor()
