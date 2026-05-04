import io
import json
import logging
import os
from datetime import datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "bronze/")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"}),
)


def ensure_bucket_exists(bucket_name: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchBucket", "NotFound"}:
            logger.info(f"Bucket '{bucket_name}' not found. Creating it...")
            s3.create_bucket(Bucket=bucket_name)
            return
        raise


def load_bronze_records() -> list[dict]:
    paginator = s3.get_paginator("list_objects_v2")
    records: list[dict] = []

    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=BRONZE_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue

            body = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read().decode("utf-8")
            payload = json.loads(body)
            if isinstance(payload, list):
                records.extend(r for r in payload if isinstance(r, dict))

    logger.info(f"Loaded {len(records)} bronze records from s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}")
    if not records:
        raise ValueError("No bronze records found to transform")
    return records


def transform_to_silver(records: list[dict]) -> list[dict]:
    transformed: dict[tuple[str, int], dict] = {}

    for r in records:
        icao24 = (r.get("icao24") or "").strip()
        last_contact = int(r.get("last_contact") or 0)
        key = (icao24, last_contact)

        transformed[key] = {
            "icao24": icao24,
            "flight_id": icao24,
            "callsign": r.get("callsign"),
            "country": r.get("country") or "UNKNOWN",
            "last_contact": last_contact,
            "longitude": r.get("longitude"),
            "latitude": r.get("latitude"),
            "altitude": r.get("altitude"),
            "on_ground": bool(r.get("on_ground")) if r.get("on_ground") is not None else False,
            "velocity": r.get("velocity"),
            "true_track": r.get("true_track"),
            "vertical_rate": r.get("vertical_rate"),
            "sensors": r.get("sensors"),
            "geo_altitude": r.get("geo_altitude"),
            "squawk": r.get("squawk"),
            "spi": bool(r.get("spi")) if r.get("spi") is not None else False,
            "position_source": int(r.get("position_source") or 0),
            "processed_at": datetime.utcnow().isoformat() + "Z",
        }

    silver_records = list(transformed.values())
    logger.info(f"Transformed to {len(silver_records)} deduplicated silver records")
    return silver_records


def write_silver(records: list[dict]) -> str:
    now = datetime.utcnow()
    partition = f"flight_states/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
    key = f"{partition}/silver_{now.strftime('%Y%m%d_%H%M%S')}.parquet"

    table = pa.Table.from_pylist(records)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer, compression="snappy")
    parquet_buffer.seek(0)

    s3.put_object(
        Bucket=SILVER_BUCKET,
        Key=key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    logger.info(f"Wrote silver parquet to s3://{SILVER_BUCKET}/{key}")
    return key


def run() -> str:
    ensure_bucket_exists(SILVER_BUCKET)
    bronze_records = load_bronze_records()
    silver_records = transform_to_silver(bronze_records)
    return write_silver(silver_records)


if __name__ == "__main__":
    run()
