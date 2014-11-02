from csv import DictReader
from StringIO import StringIO
from operator import itemgetter
from os.path import join, dirname, splitext
from os import environ

from jinja2 import Environment, FileSystemLoader

from . import S3, paths

def load_states(s3):
    # Find existing cache information
    state_key = s3.get_key('state.txt')
    states = list()

    if state_key:
        state_link = state_key.get_contents_as_string()
        state_key = s3.get_key(state_link.strip())
    
    if state_key:
        state_file = StringIO(state_key.get_contents_as_string())
        rows = DictReader(state_file, dialect='excel-tab')
        
        for row in sorted(rows, key=itemgetter('source')):
            row['shortname'], _ = splitext(row['source'])
            row['href'] = row['processed'] or row['cache'] or None

            row['class'] = ' '.join([
                'cached' if row['cache'] else '',
                'processed' if row['processed'] else '',
                ])
            
            states.append(row)
    
    return states

def main():
    s3 = S3(environ['AWS_ACCESS_KEY_ID'], environ['AWS_SECRET_ACCESS_KEY'], 'openaddresses-cfa')
    print summarize(s3)

def summarize(s3):
    ''' Return summary HTML.
    '''
    env = Environment(loader=FileSystemLoader(join(dirname(__file__), 'templates')))
    template = env.get_template('state.html')
    return template.render(states=load_states(s3))

if __name__ == '__main__':
    exit(main())
