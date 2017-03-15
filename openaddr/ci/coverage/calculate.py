import logging; _L = logging.getLogger('openaddr.ci.coverage.calculate')

from os import environ
from argparse import ArgumentParser
from urllib.parse import urljoin

import requests

from .. import setup_logger

START_URL = 'https://results.openaddresses.io/index.json'

parser = ArgumentParser(description='Calculate current worldwide address coverage.')

parser.add_argument('--sns-arn', default=environ.get('AWS_SNS_ARN', None),
                    help='Optional AWS Simple Notification Service (SNS) resource. Defaults to value of AWS_SNS_ARN environment variable.')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

def main():
    '''
    '''
    args = parser.parse_args()
    setup_logger(None, None, args.sns_arn, log_level=args.loglevel)

    index = requests.get(START_URL).json()
    geojson_url = urljoin(START_URL, index['render_geojson_url'])
    _L.info('Downloading {}...'.format(geojson_url))
