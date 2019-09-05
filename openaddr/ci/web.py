from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix

from .webauth import apply_webauth_blueprint
from .webhooks import apply_webhooks_blueprint
from .webapi import apply_webapi_blueprint
from .webcoverage import apply_coverage_blueprint
from . import load_config

app = Flask(__name__)
app.config.update(load_config())
apply_webauth_blueprint(app)
apply_webhooks_blueprint(app)
apply_webapi_blueprint(app)
apply_coverage_blueprint(app)

# Look at X-Forwarded-* request headers when behind a proxy.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=2, x_proto=2, x_port=2)
