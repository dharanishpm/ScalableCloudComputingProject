import json
import base64
import os
import time
import logging
from decimal import Decimal, InvalidOperation

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

dynamodb = boto3.resource("dynamodb")

telemetry_table = dynamodb.Table(os.environ["TELEMETRY_TABLE"])
alerts_table = dynamodb.Table(os.environ["ALERT_TABLE"])
metrics_table = dynamodb.Table(os.environ["METRICS_TABLE"])

BUCKET_SECONDS = int(os.environ.get("BUCKET_SECONDS", "60"))
WINDOW_MINUTES = int(os.environ.get("WINDOW_MINUTES", "5"))
TELEMETRY_TTL_SECONDS = int(os.environ.get("TELEMETRY_TTL_SECONDS", str(24 * 3600)))
BUCKET_TTL_SECONDS = int(os.environ.get("BUCKET_TTL_SECONDS", str(2 * 3600)))
TOP_N = int(os.environ.get("TOP_N", "5"))

# Any epoch value bigger than this is almost certainly milliseconds, not
# seconds. (10^12 seconds is year ~33658 -- safely past any real telemetry
# timestamp, but well below any millisecond timestamp from 2001 onward.)
MS_THRESHOLD = 10 ** 12

REQUIRED_FIELDS = [
    "vehicleId", "timestamp", "speed", "rpm",
    "engineLoad", "maf", "latitude", "longitude",
]

SPEED_LIMIT = 120
RPM_LIMIT = 5000
LOAD_LIMIT = 90
IDLE_RPM_LIMIT = 700


def health_score(speed, rpm, load):
    score = 100
    if speed > SPEED_LIMIT:
        score -= 20
    if rpm > RPM_LIMIT:
        score -= 30
    if load > LOAD_LIMIT:
        score -= 30
    if speed == 0 and rpm > IDLE_RPM_LIMIT:
        score -= 10
    return max(score, 0)


