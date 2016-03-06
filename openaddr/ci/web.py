from flask import Flask
from .webhooks import apply_webhooks_blueprint
from . import load_config

app = Flask(__name__)
app.config.update(load_config())
apply_webhooks_blueprint(app)
