import os

from flask import Blueprint, render_template

from . import setup_logger, webcommon

webcoverage = Blueprint('webcoverage', __name__)

@webcoverage.route('/coverage')
@webcommon.log_application_errors
def get_coverage():
    return render_template('coverage.html')

def apply_coverage_blueprint(app):
    '''
    '''
    app.register_blueprint(webcoverage)

    @app.before_first_request
    def app_prepare():
        setup_logger(None,
                     None,
                     os.environ.get('AWS_SNS_ARN'),
                     webcommon.flask_log_level(app.config))
