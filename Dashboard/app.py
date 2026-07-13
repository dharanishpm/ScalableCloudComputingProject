from flask import Flask, render_template, jsonify, request
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Initialize Flask application first
app = Flask(__name__)

# Set basic configuration
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['DEBUG'] = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'

# Global variables for helper modules
dynamodb_helper = None
athena_helper = None

# Try to load configuration and helpers
try:
    from config import Config
    app.config.from_object(Config)
    logger.info("Config loaded successfully")
except Exception as e:
    logger.warning(f"Could not load config: {e}")

try:
    import dynamodb_helper
    logger.info("DynamoDB helper loaded successfully")
except Exception as e:
    logger.warning(f"Could not load DynamoDB helper: {e}")
    dynamodb_helper = None

try:
    import athena_helper
    logger.info("Athena helper loaded successfully")
except Exception as e:
    logger.warning(f"Could not load Athena helper: {e}")
    athena_helper = None

logger.info(f"Flask app initialized successfully - Debug: {app.config.get('DEBUG', False)}")


@app.route('/health')
def health_check():
    """Simple health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'message': 'Flask application is running',
        'dynamodb_available': dynamodb_helper is not None,
        'athena_available': athena_helper is not None
    }), 200