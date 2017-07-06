import logging

from flask import Flask
from flask_cors import CORS

from specstore import make_blueprint

# Create application
app = Flask(__name__, static_folder=None)

# CORS support
CORS(app, supports_credentials=True)

# Register blueprints
app.register_blueprint(make_blueprint(), url_prefix='/source/')


logging.getLogger().setLevel(logging.INFO)

if __name__=='__main__':
    app.run()