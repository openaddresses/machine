import logging; _L = logging.getLogger('openaddr.ci.webcommon')

from urllib.parse import urlparse, urlunparse
from functools import wraps

from flask import request

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
