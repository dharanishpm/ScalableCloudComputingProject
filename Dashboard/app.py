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


@app.route('/')
def index():
    logger.info("Dashboard page accessed")
    try:
        refresh_interval = getattr(app.config, 'REFRESH_INTERVAL', 5000)
        return render_template('dashboard.html', refresh_interval=refresh_interval)
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        return f"<h1>Fleet Dashboard</h1><p>Service starting up...</p><p>Error: {e}</p>", 200


@app.route('/api/live')
def api_live():
    try:
        logger.info("Live API called")
        
        if not dynamodb_helper:
            return jsonify({
                'rollingAverageSpeed': 65.5,
                'rollingAverageRPM': 2500,
                'rollingAverageEngineLoad': 45.0,
                'fleetHealth': 85,
                'activeVehicles': 12,
                'highRPMCount': 2,
                'highEngineLoadCount': 1,
                'top5Vehicles': [
                    {'vehicleId': 'V001', 'speed': 75, 'rpm': 2800, 'engineLoad': 55},
                    {'vehicleId': 'V002', 'speed': 68, 'rpm': 2600, 'engineLoad': 48},
                    {'vehicleId': 'V003', 'speed': 62, 'rpm': 2400, 'engineLoad': 42}
                ],
                'message': 'Using mock data - DynamoDB not available'
            }), 200
        
        # Get speed layer data from DynamoDB
        fleet_summary = dynamodb_helper.get_fleet_summary()
        
        response_data = {
            'rollingAverageSpeed': fleet_summary.get('rollingAverageSpeed', 0),
            'rollingAverageRPM': fleet_summary.get('rollingAverageRPM', 0),
            'rollingAverageEngineLoad': fleet_summary.get('rollingAverageEngineLoad', 0),
            'fleetHealth': fleet_summary.get('fleetHealth', 0),
            'activeVehicles': fleet_summary.get('activeVehicles', 0),
            'highRPMCount': fleet_summary.get('highRPMCount', 0),
            'highEngineLoadCount': fleet_summary.get('highEngineLoadCount', 0),
            'top5Vehicles': fleet_summary.get('top5Vehicles', [])
        }
        
        logger.info("Live data retrieved successfully")
        return jsonify(response_data), 200
        
    except Exception as e:
        logger.error(f"Error in live API: {str(e)}")
        return jsonify({
            'error': 'Failed to retrieve live data',
            'message': str(e)
        }), 500


@app.route('/api/batch')
def api_batch():
    try:
        logger.info("Batch API called")
        
        if not athena_helper:
            return jsonify({
                'vehicles': [],
                'top5Historical': [],
                'message': 'Using mock data - Athena not available'
            }), 200
        
        # Get batch layer data from Athena
        batch_data = athena_helper.get_all_batch_data()
        top5_historical = athena_helper.get_top_vehicles_by_speed(limit=5)
        
        response_data = {
            'vehicles': batch_data,
            'top5Historical': top5_historical
        }
        
        logger.info(f"Batch data retrieved: {len(batch_data)} vehicles")
        return jsonify(response_data), 200
        
    except Exception as e:
        logger.error(f"Error in batch API: {str(e)}")
        return jsonify({
            'error': 'Failed to retrieve batch data',
            'message': str(e),
            'vehicles': [],
            'top5Historical': []
        }), 500


@app.route('/api/alerts')
def api_alerts():
    try:
        logger.info("Alerts API called")
        
        if not dynamodb_helper:
            return jsonify({
                'alerts': [],
                'message': 'Using mock data - DynamoDB not available'
            }), 200
        
        # Get recent alerts from DynamoDB
        alerts = dynamodb_helper.get_recent_alerts(limit=20)
        
        logger.info(f"Alerts retrieved: {len(alerts)} alerts")
        return jsonify({'alerts': alerts}), 200
        
    except Exception as e:
        logger.error(f"Error in alerts API: {str(e)}")
        return jsonify({
            'error': 'Failed to retrieve alerts',
            'message': str(e),
            'alerts': []
        }), 500
