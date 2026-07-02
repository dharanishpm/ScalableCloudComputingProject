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


def to_decimal(value):
    """Safely convert numeric input to Decimal for DynamoDB storage."""
    try:
        return Decimal(str(float(value)))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(f"Invalid numeric value: {value!r}")


def normalize_timestamp(raw_ts):

    ts = int(raw_ts)
    if ts > MS_THRESHOLD:
        ts //= 1000
    return ts


def parse_record(kinesis_record):
    """Decode + validate a single Kinesis record. Raises ValueError on bad data."""
    raw = base64.b64decode(kinesis_record["kinesis"]["data"])
    payload = json.loads(raw)

    missing = [f for f in REQUIRED_FIELDS if f not in payload]
    if missing:
        raise ValueError(f"Missing fields: {missing}")

    return {
        "vehicleId": str(payload["vehicleId"]),
        "timestamp": normalize_timestamp(payload["timestamp"]),
        "speed": to_decimal(payload["speed"]),
        "rpm": to_decimal(payload["rpm"]),
        "engineLoad": to_decimal(payload["engineLoad"]),
        "maf": to_decimal(payload["maf"]),
        "latitude": to_decimal(payload["latitude"]),
        "longitude": to_decimal(payload["longitude"]),
    }


def bucket_key(timestamp):
    return f"Bucket#{timestamp // BUCKET_SECONDS}"


def write_telemetry_and_alerts(item):
    """Write one telemetry row + any alert rows it triggers, batched."""
    now = int(time.time())

    health = health_score(
        float(item["speed"]), float(item["rpm"]), float(item["engineLoad"])
    )

    telemetry_item = {
        "vehicleId": item["vehicleId"],
        "timestamp": str(item["timestamp"]),
        "speed": item["speed"],
        "rpm": item["rpm"],
        "engineLoad": item["engineLoad"],
        "maf": item["maf"],
        "latitude": item["latitude"],
        "longitude": item["longitude"],
        "healthScore": Decimal(str(health)),
        "expiresAt": now + TELEMETRY_TTL_SECONDS,
    }

    alerts = []
    if item["speed"] > SPEED_LIMIT:
        alerts.append("SPEEDING")
    if item["rpm"] > RPM_LIMIT:
        alerts.append("HIGH_RPM")
    if item["engineLoad"] > LOAD_LIMIT:
        alerts.append("HIGH_ENGINE_LOAD")

    with telemetry_table.batch_writer() as batch:
        batch.put_item(Item=telemetry_item)

    if alerts:
        with alerts_table.batch_writer() as batch:
            for alert in alerts:
                batch.put_item(Item={
                    "vehicleId": item["vehicleId"],
                    "timestamp": str(item["timestamp"]),
                    "alert": alert,
                })

    return telemetry_item, health


def update_bucket_aggregate(item, health, now):
    """
    Atomically fold one record into its 1-minute tumbling-window aggregate.
    ADD is safe under concurrent writes from parallel shard invocations.
    """
    key = {"metricName": bucket_key(item["timestamp"])}
    speed = item["speed"]
    rpm = item["rpm"]
    load = item["engineLoad"]

    is_active = speed > 0
    is_high_rpm = rpm > RPM_LIMIT
    is_high_load = load > LOAD_LIMIT

    update_expr_parts = [
        "ADD speedSum :speed, speedCount :one, healthSum :health, "
        "healthCount :one, highRpmCount :hrpm, highLoadCount :hload, "
        "rpmSum :rpm, loadSum :load"
    ]
    values = {
        ":speed": speed,
        ":one": 1,
        ":health": Decimal(str(health)),
        ":hrpm": 1 if is_high_rpm else 0,
        ":hload": 1 if is_high_load else 0,
        # NOTE: rpmSum / loadSum were previously missing entirely, which is
        # why rolling average RPM / engine load could never be computed --
        # only the *count* of high-RPM/high-load events existed, never a
        # running sum to average. speedCount doubles as the shared
        # denominator for these too, since every record contributes exactly
        # one unit to every sum in the same update.
        ":rpm": rpm,
        ":load": load,
    }


