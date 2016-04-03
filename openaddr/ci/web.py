from flask import Flask
from werkzeug.contrib.fixers import ProxyFix

from .webhooks import apply_webhooks_blueprint
from .webapi import apply_webapi_blueprint
from . import load_config

app = Flask(__name__)
app.config.update(load_config())
apply_webhooks_blueprint(app)
apply_webapi_blueprint(app)

# Look at X-Forwarded-* request headers when behind a proxy.
app.wsgi_app = ProxyFix(app.wsgi_app)
