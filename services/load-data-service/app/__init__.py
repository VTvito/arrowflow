from common.logging_config import configure_service_logging
from flask import Flask, jsonify

from app.routes import bp

SERVICE_NAME = 'load-data-service'


def create_app():
    app = Flask(__name__)
    app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024
    app.register_blueprint(bp)
    configure_service_logging(SERVICE_NAME)

    @app.errorhandler(404)
    def not_found(_e):
        return jsonify({"status": "error", "message": "Not found"}), 404

    @app.errorhandler(413)
    def payload_too_large(_e):
        return jsonify({"status": "error", "message": "Payload too large"}), 413

    return app
