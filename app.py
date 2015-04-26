from sys import stderr
from flask import Flask, request

app = Flask(__name__)

@app.route('/')
def index():
    return 'Yo.'

@app.route('/hook', methods=['GET', 'POST'])
def hook():
    print >> stderr, request.method
    print >> stderr, request.headers
    print >> stderr, request.values
    print >> stderr, repr(request.data)

    return 'Yo.'

if __name__ == '__main__':
    app.run(debug=True)
