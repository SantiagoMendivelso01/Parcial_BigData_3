"""
01_clean_deduplicate.py — Limpieza y deduplicacion
spark-submit 01_clean_deduplicate.py --date 2024-01-15 --bucket shopstream-XXXX
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

SCHEMAS = {
    "page_view": StructType([
        StructField("event_type",           StringType(),  True),
        StructField("user_id",              StringType(),  True),
        StructField("session_id",           StringType(),  True),
        StructField("page_url",             StringType(),  True),
        StructField("page_type",            StringType(),  True),
        StructField("timestamp",            StringType(),  True),
        StructField("time_on_page_seconds", IntegerType(), True),
        StructField("referrer",             StringType(),  True),
        StructField("device_type",          StringType(),  True),
        StructField("country",              StringType(),  True),
    ]),
    "click": StructType([
        StructField("event_type",   StringType(),  True),
        StructField("user_id",      StringType(),  True),
        StructField("session_id",   StringType(),  True),
        StructField("element_id",   StringType(),  True),
        StructField("element_type", StringType(),  True),
        StructField("page_url",     StringType(),  True),
        StructField("timestamp",    StringType(),  True),
        StructField("x_position",   IntegerType(), True),
        StructField("y_position",   IntegerType(), True),
    ]),
    "search": StructType([
        StructField("event_type",    StringType(),  True),
        StructField("user_id",       StringType(),  True),
        StructField("session_id",    StringType(),  True),
        StructField("query",         StringType(),  True),
        StructField("results_count", IntegerType(), True),
        StructField("timestamp",     StringType(),  True),
    ]),
    "product_view": StructType([
        StructField("event_type",           StringType(),  True),
        StructField("user_id",              StringType(),  True),
        StructField("session_id",           StringType(),  True),
        StructField("product_id",           StringType(),  True),
        StructField("category",             StringType(),  True),
        StructField("price",                DoubleType(),  True),
        StructField("timestamp",            StringType(),  True),
        StructField("time_on_page_seconds", IntegerType(), True),
    ]),
    "cart_event": StructType([
        StructField("event_type", StringType(), True),
        StructField("user_id",    StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("action",     StringType(), True),
        StructField("timestamp",  StringType(), True),
    ]),
}

IMPUTATION = {
    "referrer":             "direct",
    "device_type":          "unknown",
    "country":              "XX",
    "page_type":            "other",
    "category":             "other",
    "price":                0.0,
    "time_on_page_seconds": 0,
    "results_count":        0,
    "x_position":           0,
    "y_position":           0,
}


def create_spark():
    return (
        SparkSession.builder
        .appName("ShopStream-Clean-Deduplicate")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def clean_dataframe(df, event_type):
    # 1. Deduplicacion exacta
    df = df.dropDuplicates()

    # 2. Eliminar registros sin campos clave
    df = df.dropna(subset=["user_id", "session_id", "timestamp"])

    # 3. Imputar nulos
    for col_name, default_val in IMPUTATION.items():
        if col_name in df.columns:
            df = df.fillna({col_name: default_val})

    # 4. Parsear timestamp
    df = df.withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX")
    ).drop("timestamp")

    # 5. Filtrar timestamps invalidos
    df = df.filter(F.col("event_timestamp").isNotNull())
    df = df.filter(F.col("event_timestamp") <= F.current_timestamp())

    # 6. Normalizar strings
    for col_name in ["page_type", "device_type", "country", "category", "action"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.lower(F.trim(F.col(col_name))))

    # 7. Agregar columna de fecha
    df = df.withColumn("event_date", F.to_date(F.col("event_timestamp")))

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",   required=True)
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    dt     = datetime.strptime(args.date, "%Y-%m-%d")
    s3_raw = f"s3://{args.bucket}/raw"
    s3_out = f"s3://{args.bucket}/processed/clean"

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\nShopStream - Limpieza para {args.date}")

    for event_type, schema in SCHEMAS.items():
        print(f"\nProcesando: {event_type}")
        try:
            path = f"{s3_raw}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{event_type}.jsonl"
            df_raw   = spark.read.schema(schema).json(path)
            raw_count = df_raw.count()

            df_clean  = clean_dataframe(df_raw, event_type)
            clean_count = df_clean.count()

            out_path = f"{s3_out}/{event_type}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
            df_clean.write.mode("overwrite").parquet(out_path)

            print(f"  {event_type}: {raw_count:,} raw -> {clean_count:,} limpios")
        except Exception as e:
            print(f"  ERROR en {event_type}: {e}")

    spark.stop()


if __name__ == "__main__":
    main()
