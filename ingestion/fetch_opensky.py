import requests
import json
import logging
import os
import time
from datetime import datetime
from pydantic import BaseModel, ValidationError
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# MinIO/S3 Config
# In docker-compose, Airflow should talk to MinIO via service DNS name.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    config=Config(signature_version='s3v4', retries={'max_attempts': 3, 'mode': 'standard'})
)


def ensure_bucket_exists(bucket_name: str) -> None:
    """Create the bucket if it does not exist yet."""
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchBucket", "NotFound"}:
            logger.info(f"Bucket '{bucket_name}' not found. Creating it...")
            s3.create_bucket(Bucket=bucket_name)
            return
        raise

class FlightRecord(BaseModel):
    icao24: str
    callsign: str | None
    country: str
    last_contact: int
    longitude: float | None
    latitude: float | None
    altitude: float | None
    on_ground: bool
    velocity: float | None
    true_track: float | None
    vertical_rate: float | None
    sensors: list[int] | None
    geo_altitude: float | None
    squawk: str | None
    spi: bool
    position_source: int

def fetch_opensky(max_retries=3, backoff=2):
    """Fetch OpenSky data with exponential backoff retry logic"""
    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching OpenSky API (attempt {attempt+1}/{max_retries})...")
            resp = requests.get("https://opensky-network.org/api/states/all", timeout=30)
            resp.raise_for_status()
            logger.info("API fetch successful")
            return resp.json().get("states", [])
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                wait = backoff ** attempt
                logger.info(f"Retrying in {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"All {max_retries} attempts failed")
                return []
    return []

def validate_and_upload(states):
    """Validate and upload with robust error handling"""
    if not states:
        logger.warning("No states received from API")
        return None
        
    valid, invalid = [], []
    
    for i, s in enumerate(states):
        try:
            # Defensive parsing: handle missing/None fields
            record = FlightRecord(
                icao24=str(s[0]) if s[0] is not None else "",
                callsign=str(s[1]).strip() if s[1] else None,
                country=str(s[2]) if s[2] else "UNKNOWN",
                last_contact=int(s[3]) if s[3] is not None else 0,
                longitude=float(s[5]) if s[5] is not None else None,
                latitude=float(s[6]) if s[6] is not None else None,
                altitude=float(s[7]) if s[7] is not None else None,
                on_ground=bool(s[8]) if s[8] is not None else False,
                velocity=float(s[9]) if s[9] is not None else None,
                true_track=float(s[10]) if s[10] is not None else None,
                vertical_rate=float(s[11]) if s[11] is not None else None,
                sensors=s[12] if isinstance(s[12], list) else None,
                geo_altitude=float(s[13]) if s[13] is not None else None,
                squawk=str(s[14]).strip() if s[14] else None,
                spi=bool(s[15]) if s[15] is not None else False,
                position_source=int(s[16]) if s[16] is not None else 0
            )
            valid.append(record.model_dump())
        except (ValidationError, IndexError, TypeError, ValueError) as e:
            logger.debug(f"Skipping record {i}: {e}")
            invalid.append(i)
    
    logger.info(f"{len(valid)} valid | {len(invalid)} invalid records")
    if not valid:
        logger.warning("No valid records to upload")
        return None

    # Partition by UTC time
    now = datetime.utcnow()
    partition = f"bronze/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
    key = f"{partition}/flights_{now.strftime('%Y%m%d_%H%M%S')}.json"

    try:
        ensure_bucket_exists(BRONZE_BUCKET)
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=json.dumps(valid).encode("utf-8"),
            ContentType="application/json"
        )
        logger.info(f"Uploaded {len(valid)} records to s3://{BRONZE_BUCKET}/{key}")
        return key
    except ClientError as e:
        logger.error(f"S3 upload failed: {e}")
        raise  # Let Airflow retry
    except EndpointConnectionError as e:
        logger.error(f"Could not connect to MinIO endpoint '{MINIO_ENDPOINT}': {e}")
        raise

if __name__ == "__main__":
    states = fetch_opensky()
    validate_and_upload(states)
