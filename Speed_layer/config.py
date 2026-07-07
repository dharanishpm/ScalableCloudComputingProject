"""
Configuration for Fleet Data Producer
"""
import os

# AWS Configuration
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Kinesis Configuration
KINESIS_STREAM_NAME = os.environ.get('KINESIS_STREAM_NAME', 'fleet-stream')

# Data Configuration
CSV_FILE = os.environ.get('CSV_FILE', 'vehicle_data.csv')
SLEEP_INTERVAL = float(os.environ.get('SLEEP_INTERVAL', '1.0'))  # seconds between records

# Producer Settings
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '1'))  # Number of records to send in batch
ENABLE_HEALTH_SCORING = os.environ.get('ENABLE_HEALTH_SCORING', 'true').lower() == 'true'

# Health Score Thresholds
SPEED_WARNING_THRESHOLD = int(os.environ.get('SPEED_WARNING_THRESHOLD', '100'))
SPEED_CRITICAL_THRESHOLD = int(os.environ.get('SPEED_CRITICAL_THRESHOLD', '120'))
RPM_WARNING_THRESHOLD = int(os.environ.get('RPM_WARNING_THRESHOLD', '4000'))
RPM_CRITICAL_THRESHOLD = int(os.environ.get('RPM_CRITICAL_THRESHOLD', '5000'))
ENGINE_LOAD_WARNING_THRESHOLD = int(os.environ.get('ENGINE_LOAD_WARNING_THRESHOLD', '75'))
ENGINE_LOAD_CRITICAL_THRESHOLD = int(os.environ.get('ENGINE_LOAD_CRITICAL_THRESHOLD', '90'))