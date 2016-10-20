from zipfile import ZipFile
from os.path import splitext
from csv import DictReader
from io import TextIOWrapper

from .. import iterate_local_processed_files

def iterate_runs_points(runs):
    '''
    '''
    for result in iterate_local_processed_files(runs):
        print('source_base:', result.source_base)
        print('filename:', result.filename)
        print('run_state:', result.run_state)
        print('code_version:', result.code_version)
        with open(result.filename, 'rb') as file:
            result_zip = ZipFile(file)
            
            csv_infos = [zipinfo for zipinfo in result_zip.infolist()
                         if splitext(zipinfo.filename)[1] == '.csv']
            
            if not csv_infos:
                break

            zipped_file = result_zip.open(csv_infos[0].filename)
            point_rows = DictReader(TextIOWrapper(zipped_file))
            
            for row in point_rows:
                yield row
