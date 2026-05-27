"""
04_navigation_paths.py — Rutas de navegacion mas frecuentes
spark-submit 04_navigation_paths.py --date 2024-01-15 --bucket shopstream-XXXX
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def create_spark():
    return (
        SparkSession.builder
        .appName("ShopStream-NavPaths")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",   required=True)
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    dt    = datetime.strptime(args.date, "%Y-%m-%d")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\nShopStream - Rutas de navegacion para {args.date}")

    path  = f"s3://{args.bucket}/processed/clean/page_view/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    df_pv = spark.read.parquet(path)

    window = Window.partitionBy("session_id").orderBy("event_timestamp")

    df_paths = (
        df_pv
        .withColumn("page_sequence", F.collect_list("page_type").over(window))
        .groupBy("session_id")
        .agg(F.max("page_sequence").alias("path"))
        .filter(F.size(F.col("path")) >= 2)
        .withColumn("path_string", F.concat_ws(" -> ", F.slice(F.col("path"), 1, 6)))
    )

    result = (
        df_paths
        .groupBy("path_string")
        .agg(F.count("session_id").alias("session_count"))
        .orderBy(F.col("session_count").desc())
        .limit(10)
        .withColumnRenamed("path_string", "navigation_path")
    )

    result.show(truncate=False)

    out = f"s3://{args.bucket}/processed/metrics/navigation_paths/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    result.write.mode("overwrite").parquet(out)
    print(f"OK navigation_paths: {result.count()} filas")

    spark.stop()


if __name__ == "__main__":
    main()
