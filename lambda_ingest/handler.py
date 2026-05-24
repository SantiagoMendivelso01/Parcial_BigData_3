"""
handler.py — Lambda de ingesta y validación
Se activa con eventos S3 PutObject cuando llega un archivo nuevo al bucket.
"""

import json
import os
import urllib.parse
from datetime import datetime, timezone

import boto3
from validators import validate_records, ValidationResult

s3 = boto3.client("s3")
cw = boto3.client("cloudwatch", region_name=os.environ.get("AWS_REGION", "us-east-1"))

BUCKET        = os.environ.get("BUCKET", "")
METRIC_NAMESPACE = "ShopStream/Ingest"


def put_metric(name, value, unit="Count"):
    try:
        cw.put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[{
                "MetricName": name,
                "Value": value,
                "Unit": unit,
                "Timestamp": datetime.now(timezone.utc),
            }]
        )
    except Exception as e:
        print(f"[WARN] No se pudo publicar metrica {name}: {e}")


def read_s3_file(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    records = []
    for line in body.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as e:
            print(f"[WARN] Linea no es JSON valido: {e}")
    return records


def move_to_quarantine(bucket, source_key, errors):
    quarantine_key = f"quarantine/{source_key}"
    error_key      = f"quarantine/{source_key}.errors.json"

    # Copiar archivo original a quarantine/
    s3.copy_object(
        CopySource={"Bucket": bucket, "Key": source_key},
        Bucket=bucket,
        Key=quarantine_key,
    )

    # Guardar metadata del error
    error_metadata = {
        "source_key": source_key,
        "quarantined_at": datetime.now(timezone.utc).isoformat(),
        "error_count": len(errors),
        "errors": errors[:100],
    }
    s3.put_object(
        Bucket=bucket,
        Key=error_key,
        Body=json.dumps(error_metadata, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"[QUARANTINE] {source_key} -> {quarantine_key}")
    return quarantine_key


def handler(event, context):
    processed    = 0
    errors_total = 0

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key    = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        size   = record["s3"]["object"].get("size", 0)

        print(f"[INFO] Procesando s3://{bucket}/{key} ({size} bytes)")

        # Evitar loop si el archivo ya está en quarantine
        if key.startswith("quarantine/"):
            print("[SKIP] Archivo ya en quarantine, ignorando.")
            continue

        try:
            records = read_s3_file(bucket, key)
            total   = len(records)

            if not records:
                print(f"[WARN] Archivo vacio: {key}")
                put_metric("EmptyFiles", 1)
                continue

            # El tipo de evento viene del nombre del archivo (page_view.jsonl -> page_view)
            event_type = key.split("/")[-1].replace(".jsonl", "")

            result = validate_records(records, event_type)
            print(f"[INFO] Validos: {result.valid_count} | Invalidos: {result.invalid_count}")

            if result.invalid_count > 0:
                errors_total += result.invalid_count
                move_to_quarantine(bucket, key, result.errors)
                put_metric("FilesWithErrors", 1)
                put_metric("InvalidRecords", result.invalid_count)

            put_metric("FilesProcessed", 1)
            put_metric("ValidRecords", result.valid_count)
            put_metric("FileSizeBytes", size, unit="Bytes")

            processed += 1

        except Exception as e:
            print(f"[ERROR] Fallo procesando {key}: {e}")
            put_metric("ProcessingErrors", 1)
            raise

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed_files": processed,
            "total_errors": errors_total,
        })
    }
