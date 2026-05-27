"""
upload_to_s3.py
Sube los archivos generados por generate_events.py al bucket S3 raw.

Uso:
    python upload_to_s3.py --date 2024-01-15 --bucket shopstream-raw
"""

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def get_s3_key(date, event_type):
    return (
        f"raw/"
        f"year={date.year}/"
        f"month={date.month:02d}/"
        f"day={date.day:02d}/"
        f"{event_type}.jsonl"
    )


def upload_file(s3_client, local_path, bucket, s3_key):
    try:
        file_size = local_path.stat().st_size
        print(f"  Subiendo {local_path.name} ({file_size / 1024 / 1024:.1f} MB)...")
        s3_client.upload_file(
            str(local_path),
            bucket,
            s3_key,
            ExtraArgs={"ContentType": "application/x-ndjson"},
        )
        print(f"    OK s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        print(f"    ERROR subiendo {local_path.name}: {e}")
        return False


def upload_day(date, input_dir, bucket, region):
    partition = (
        input_dir
        / f"year={date.year}"
        / f"month={date.month:02d}"
        / f"day={date.day:02d}"
    )

    if not partition.exists():
        raise FileNotFoundError(f"Directorio no encontrado: {partition}")

    s3 = boto3.client("s3", region_name=region)
    results = {"uploaded": 0, "failed": 0}

    for jsonl_file in sorted(partition.glob("*.jsonl")):
        event_type = jsonl_file.stem
        s3_key     = get_s3_key(date, event_type)

        if upload_file(s3, jsonl_file, bucket, s3_key):
            results["uploaded"] += 1
        else:
            results["failed"] += 1

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",   default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    parser.add_argument("--input",  default="./output")
    parser.add_argument("--bucket", default=os.getenv("S3_RAW_BUCKET", "shopstream-raw"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    args = parser.parse_args()

    date      = datetime.strptime(args.date, "%Y-%m-%d")
    input_dir = Path(args.input)

    print(f"\nShopStream - Subiendo datos del {args.date} a s3://{args.bucket}")
    results = upload_day(date, input_dir, args.bucket, args.region)

    print(f"\nSubidos: {results['uploaded']} | Fallidos: {results['failed']}")
    if results["failed"] > 0:
        exit(1)


if __name__ == "__main__":
    main()
