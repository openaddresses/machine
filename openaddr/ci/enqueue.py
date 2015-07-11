from os import environ

from . import enqueue_sources

auth = environ['GITHUB_TOKEN'], 'x-oauth-basic'
start_url = 'https://api.github.com/repos/openaddresses/openaddresses'
start_url = 'https://api.github.com/repos/openaddresses/hooked-on-sources'
enqueue_sources(start_url, auth)
