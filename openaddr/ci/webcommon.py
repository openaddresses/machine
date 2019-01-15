import logging; _L = logging.getLogger('openaddr.ci.webcommon')

from urllib.parse import urlparse, urlunparse
from functools import wraps

from flask import request

import os, time
import boto

def log_application_errors(route_function):
    ''' Error-logging decorator for route functions.

        Don't do much, but get an error out to the logger.
    '''
    @wraps(route_function)
    def decorated_function(*args, **kwargs):
        try:
            return route_function(*args, **kwargs)
        except Exception as e:
            request_info = ' '.join([request.method, request.path])
            _L.error(e, extra={'request_info': request_info}, exc_info=True)
            raise

    return decorated_function

def monitor_execution_time(route_function):
    ''' Time-monitoring decorator for route functions.
    '''
    try:
        # Rely on boto environment.
        cw = boto.connect_cloudwatch()
    except:
        cw = False
    
    @wraps(route_function)
    def decorated_function(*args, **kwargs):
        start_time = time.time()
        result = route_function(*args, **kwargs)
        if cw:
            ns = os.environ.get('AWS_CLOUDWATCH_NS')
            fmod, fname = route_function.__module__, route_function.__name__
            metric, elapsed = f'timing {fmod}.{fname}', (time.time() - start_time)
            cw.put_metric_data(ns, metric, elapsed, unit='Seconds')
        return result

    return decorated_function

def flask_log_level(config):
    '''
    '''
    return config.get('MINIMUM_LOGLEVEL', logging.WARNING)

def nice_domain(url):
    '''
    '''
    parsed = urlparse(url)
    _ = None

    if parsed.hostname == u'data.openaddresses.io':
        return urlunparse((u'http', parsed.hostname, parsed.path, _, _, _))

    if parsed.hostname == u's3.amazonaws.com' and parsed.path.startswith(u'/data.openaddresses.io/'):
        return urlunparse((u'http', u'data.openaddresses.io', parsed.path[22:], _, _, _))

    if parsed.hostname == u'data.openaddresses.io.s3.amazonaws.com':
        return urlunparse((u'http', u'data.openaddresses.io', parsed.path, _, _, _))

    return url
