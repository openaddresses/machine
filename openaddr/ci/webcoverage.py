import os, re
import psycopg2
import psycopg2.extras

from flask import Blueprint, render_template

from . import setup_logger, webcommon

webcoverage = Blueprint('webcoverage', __name__)

@webcoverage.route('/coverage/')
@webcoverage.route('/coverage/world/')
@webcommon.log_application_errors
def get_coverage():
    with psycopg2.connect(os.environ['DATABASE_URL']) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as db:
            db.execute('''SELECT iso_a2, name, addr_count, area_total,
                                 area_pct, pop_total, pop_pct
                          FROM areas WHERE name IS NOT NULL ORDER BY name''')
            areas = db.fetchall()
            
    best_areas, okay_areas, empty_areas = list(), list(), list()
    
    for area in areas:
        if area['pop_pct'] > 0.98:
            best_areas.append(area)
        elif area['pop_pct'] > 0.15:
            okay_areas.append(area)
        else:
            empty_areas.append(area)
    
    return render_template('coverage-world.html', best_areas=best_areas,
                           okay_areas=okay_areas, empty_areas=empty_areas)

@webcoverage.route('/coverage/us/')
@webcommon.log_application_errors
def get_us_coverage():
    with psycopg2.connect(os.environ['DATABASE_URL']) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as db:
            db.execute('''SELECT usps_code, name, addr_count, area_total,
                                 area_pct, pop_total, pop_pct
                          FROM us_states WHERE name IS NOT NULL ORDER BY name''')
            areas = db.fetchall()
            
    best_areas, okay_areas, empty_areas = list(), list(), list()
    
    for area in areas:
        if area['pop_pct'] > 0.98:
            best_areas.append(area)
        elif area['pop_pct'] > 0.15:
            okay_areas.append(area)
        else:
            empty_areas.append(area)
    
    return render_template('coverage-us.html', best_areas=best_areas,
                           okay_areas=okay_areas, empty_areas=empty_areas)

def filter_nice_flag(iso_a2):
    ''' Format a floating point number like '11%'
    '''
    chars = [0x1F1A5 + ord(letter) for letter in iso_a2]
    return '&#{};&#{};'.format(*chars)

def filter_nice_percentage(number):
    ''' Format a floating point number like '11%'
    '''
    if number >= 0.99:
        return '{:.0f}%'.format(number * 100)
    
    return '{:.1f}%'.format((number or 0) * 100)

def filter_nice_big_number(number):
    ''' Format a number like '99M', '9.9M', '99K', '9.9K', or '999'
    '''
    if number > 1000000:
        return '{}K'.format(filter_nice_integer(number / 1000))
    
    if number > 10000000:
        return '{:.0f}M'.format(number / 1000000)
    
    if number > 1000000:
        return '{:.1f}M'.format(number / 1000000)
    
    if number > 10000:
        return '{:.0f}K'.format(number / 1000)
    
    if number > 1000:
        return '{:.1f}K'.format(number / 1000)
    
    if number >= 1:
        return '{:.0f}'.format(number)
    
    return '0'

def filter_nice_integer(number):
    ''' Format a number like '999,999,999'
    '''
    string = str(int(number))
    pattern = re.compile(r'^(\d+)(\d\d\d)\b')
    
    while pattern.match(string):
        string = pattern.sub(r'\1,\2', string)
    
    return string

def apply_coverage_blueprint(app):
    '''
    '''
    app.register_blueprint(webcoverage)

    @app.before_first_request
    def app_prepare():
        # Filters are set here so Jinja debug reload works; see also:
        # https://github.com/pallets/flask/issues/1907#issuecomment-225743376
        app.jinja_env.filters['nice_flag'] = filter_nice_flag
        app.jinja_env.filters['nice_percentage'] = filter_nice_percentage
        app.jinja_env.filters['nice_big_number'] = filter_nice_big_number
        app.jinja_env.filters['nice_coverage_integer'] = filter_nice_integer

        setup_logger(os.environ.get('AWS_SNS_ARN'), None, webcommon.flask_log_level(app.config))
