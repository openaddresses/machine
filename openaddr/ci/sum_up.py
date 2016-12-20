import logging; _L = logging.getLogger('openaddr.ci.collect')

from ..compat import standard_library

from argparse import ArgumentParser
from urllib.parse import urlparse
from datetime import date
from os import environ

from .objects import read_latest_set, read_completed_runs_to_date
from . import db_connect, db_cursor, setup_logger, render_index_maps, log_function_errors, dashboard_stats
from .. import S3, util

parser = ArgumentParser(description='Run some source files.')

parser.add_argument('-o', '--owner', default='openaddresses',
                    help='Github repository owner. Defaults to "openaddresses".')

parser.add_argument('-r', '--repository', default='openaddresses',
                    help='Github repository name. Defaults to "openaddresses".')

parser.add_argument('-b', '--bucket', default=environ.get('AWS_S3_BUCKET', None),
                    help='S3 bucket name. Defaults to value of AWS_S3_BUCKET environment variable.')

parser.add_argument('-d', '--database-url', default=environ.get('DATABASE_URL', None),
                    help='Optional connection string for database. Defaults to value of DATABASE_URL environment variable.')

parser.add_argument('--sns-arn', default=environ.get('AWS_SNS_ARN', None),
                    help='Optional AWS Simple Notification Service (SNS) resource. Defaults to value of AWS_SNS_ARN environment variable.')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

@log_function_errors
def main():
    ''' Single threaded worker to serve the job queue.
    '''
    args = parser.parse_args()
    setup_logger(None, None, args.sns_arn, log_level=args.loglevel)
    s3 = S3(None, None, args.bucket)
    db_args = util.prepare_db_kwargs(args.database_url)

    with db_connect(**db_args) as conn:
        with db_cursor(conn) as db:
            set = read_latest_set(db, args.owner, args.repository)
            runs = read_completed_runs_to_date(db, set.id)
            stats = dashboard_stats.make_stats(db)

    render_index_maps(s3, runs)
    dashboard_stats.upload_stats(s3, stats)

if __name__ == '__main__':
    exit(main())
