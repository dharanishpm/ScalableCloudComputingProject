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
    """
    Accept either epoch seconds or epoch milliseconds (whatever the
    producer sends) and always return epoch SECONDS as an int.

    This is the critical fix: every downstream bucket-key computation
    (write side in update_bucket_aggregate, read side in
    get_window_buckets) assumes seconds. If the two sides disagree on
    units, buckets never line up and FleetSummary never updates.
    """
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

    if is_active:
        update_expr_parts[0] += ", activeVehicles :veh"
        values[":veh"] = {item["vehicleId"]}

    update_expr = update_expr_parts[0] + " SET expiresAt = :exp"
    values[":exp"] = now + BUCKET_TTL_SECONDS

    metrics_table.update_item(
        Key=key,
        UpdateExpression=update_expr,
        ExpressionAttributeValues=values,
    )

    maybe_update_bucket_top_n(key, item["vehicleId"], speed, rpm, load, now)


def maybe_update_bucket_top_n(key, vehicle_id, speed, rpm, load, now):
    """
    Keep a small top-N speed candidate list per bucket. Only reads/writes
    when this record could plausibly make the top N, to avoid a
    read-modify-write on every single record.

    NOTE: this is a best-effort, non-atomic read-modify-write. Under
    heavy concurrent writes from parallel shard invocations, two
    records racing on the same bucket can occasionally clobber each
    other's candidate. That's an accepted approximation for a speed
    layer (see module docstring) -- if you need it to be exact, use a
    DynamoDB transaction (TransactWriteItems with a ConditionExpression
    on a version attribute) instead, at the cost of extra latency/cost.
    """
    response = metrics_table.get_item(
        Key=key, ProjectionExpression="topSpeeds"
    )
    current = response.get("Item", {}).get("topSpeeds", [])

    if len(current) >= TOP_N and speed <= min(Decimal(c["speed"]) for c in current):
        return

    candidates = list(current) + [
        {"vehicleId": vehicle_id, "speed": speed, "rpm": rpm, "engineLoad": load}
    ]
    candidates.sort(key=lambda c: float(c["speed"]), reverse=True)
    top_n = candidates[:TOP_N]

    metrics_table.update_item(
        Key=key,
        UpdateExpression="SET topSpeeds = :t, expiresAt = :exp",
        ExpressionAttributeValues={
            ":t": top_n,
            ":exp": now + BUCKET_TTL_SECONDS,
        },
    )


def get_window_buckets(now):
    """Fetch the last WINDOW_MINUTES bucket-aggregate items via BatchGetItem."""
    current_bucket = now // BUCKET_SECONDS
    bucket_count = max(1, (WINDOW_MINUTES * 60) // BUCKET_SECONDS)
    keys = [
        {"metricName": f"Bucket#{current_bucket - i}"}
        for i in range(bucket_count)
    ]

    logger.debug(
        "Looking up window buckets: %s", [k["metricName"] for k in keys]
    )

    items = []
    table_name = os.environ["METRICS_TABLE"]
    # BatchGetItem caps at 100 keys per call; window sizes here are tiny,
    # but chunk defensively in case WINDOW_MINUTES is set very large.
    for i in range(0, len(keys), 100):
        chunk = keys[i:i + 100]
        response = dynamodb.batch_get_item(
            RequestItems={table_name: {"Keys": chunk}}
        )
        items.extend(response.get("Responses", {}).get(table_name, []))

    logger.info("Fetched %d/%d window buckets", len(items), len(keys))
    return items


def combine_window(buckets, now):
    """Merge per-minute bucket aggregates into one sliding-window summary."""
    speed_sum = Decimal("0")
    speed_count = 0
    health_sum = Decimal("0")
    health_count = 0
    rpm_sum = Decimal("0")
    load_sum = Decimal("0")
    high_rpm = 0
    high_load = 0
    active_vehicles = set()
    top_candidates = []

    for b in buckets:
        speed_sum += b.get("speedSum", Decimal("0"))
        speed_count += int(b.get("speedCount", 0))
        health_sum += b.get("healthSum", Decimal("0"))
        health_count += int(b.get("healthCount", 0))
        rpm_sum += b.get("rpmSum", Decimal("0"))
        load_sum += b.get("loadSum", Decimal("0"))
        high_rpm += int(b.get("highRpmCount", 0))
        high_load += int(b.get("highLoadCount", 0))
        active_vehicles |= set(b.get("activeVehicles", set()))
        top_candidates.extend(b.get("topSpeeds", []))

    if speed_count == 0:
        return None

    top_candidates.sort(key=lambda c: float(c["speed"]), reverse=True)
    top_n = top_candidates[:TOP_N]

    return {
        "metricName": "FleetSummary",
        "rollingAverageSpeed": Decimal(str(round(float(speed_sum / speed_count), 2))),
        "rollingAverageRPM": Decimal(str(round(float(rpm_sum / speed_count), 2))),
        "rollingAverageEngineLoad": Decimal(str(round(float(load_sum / speed_count), 2))),
        "activeVehicles": len(active_vehicles),
        "highRPMCount": high_rpm,
        "highEngineLoadCount": high_load,
        "fleetHealth": Decimal(str(round(float(health_sum / health_count), 2))) if health_count else Decimal("0"),
        "top5Vehicles": [
            {
                "vehicleId": c["vehicleId"],
                "speed": Decimal(str(c["speed"])),
                "rpm": Decimal(str(c.get("rpm", 0))),
                "engineLoad": Decimal(str(c.get("engineLoad", 0))),
            }
            for c in top_n
        ],
        "updatedAt": str(now),
    }


def lambda_handler(event, context):
    batch_item_failures = []
    processed = 0
    now = int(time.time())

    for record in event["Records"]:
        try:
            item = parse_record(record)
            _, health = write_telemetry_and_alerts(item)
            update_bucket_aggregate(item, health, now)
            processed += 1
        except Exception:
            logger.exception(
                "Failed to process record %s", record.get("kinesis", {}).get("sequenceNumber")
            )
            # Report this specific record as failed so Kinesis retries only it,
            # instead of the whole batch (requires "report batch item failures"
            # to be enabled on the event source mapping).
            batch_item_failures.append({
                "itemIdentifier": record["kinesis"]["sequenceNumber"]
            })

    try:
        buckets = get_window_buckets(now)
        summary = combine_window(buckets, now)
        if summary:
            metrics_table.put_item(Item=summary)
            logger.info("FleetSummary updated: %s", summary)
        else:
            logger.info("No data in the current %s-minute window", WINDOW_MINUTES)
    except Exception:
        # Windowed metrics are best-effort; don't fail the whole batch over them.
        logger.exception("Failed to compute/write fleet summary")

    return {
        "statusCode": 200,
        "processed": processed,
        "batchItemFailures": batch_item_failures,
    }