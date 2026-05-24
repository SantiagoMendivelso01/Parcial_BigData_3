"""
02_metrics_aggregation.py — Metricas de comportamiento
spark-submit 02_metrics_aggregation.py --date 2024-01-15 --bucket shopstream-XXXX
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark():
    return (
        SparkSession.builder
        .appName("ShopStream-Metrics")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def read_clean(spark, bucket, event_type, dt):
    path = f"s3://{bucket}/processed/clean/{event_type}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    return spark.read.parquet(path)


def write_metric(df, bucket, name, dt):
    path = f"s3://{bucket}/processed/metrics/{name}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    df.write.mode("overwrite").parquet(path)
    print(f"  OK {name}: {df.count():,} filas")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",   required=True)
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    dt    = datetime.strptime(args.date, "%Y-%m-%d")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\nShopStream - Metricas para {args.date}")

    df_pv = read_clean(spark, args.bucket, "page_view", dt)
    df_pv.cache()

    # Top 20 paginas por tiempo de permanencia
    print("Calculando top paginas...")
    top_pages = (
        df_pv
        .groupBy("page_url", "page_type")
        .agg(
            F.avg("time_on_page_seconds").alias("avg_time_seconds"),
            F.count("*").alias("total_visits"),
        )
        .filter(F.col("total_visits") >= 5)
        .orderBy(F.col("avg_time_seconds").desc())
        .limit(20)
    )
    write_metric(top_pages, args.bucket, "top_pages_by_time", dt)

    # Tasa de rebote por tipo de pagina
    print("Calculando tasa de rebote...")
    session_counts = (
        df_pv
        .groupBy("session_id", "page_type")
        .agg(F.count("*").alias("pv_count"))
    )
    bounce = session_counts.filter(F.col("pv_count") == 1)
    total_by_type = session_counts.groupBy("page_type").agg(F.count("session_id").alias("total_sessions"))
    bounce_by_type = bounce.groupBy("page_type").agg(F.count("session_id").alias("bounce_sessions"))
    bounce_rate = (
        total_by_type
        .join(bounce_by_type, "page_type", "left")
        .fillna({"bounce_sessions": 0})
        .withColumn("bounce_rate_pct", F.round(F.col("bounce_sessions") / F.col("total_sessions") * 100, 2))
        .orderBy("page_type")
    )
    write_metric(bounce_rate, args.bucket, "bounce_rate", dt)

    # Tiempo promedio por dispositivo y pais
    print("Calculando tiempo por dispositivo y pais...")
    device_country = (
        df_pv
        .groupBy("device_type", "country")
        .agg(
            F.avg("time_on_page_seconds").alias("avg_time_seconds"),
            F.count("session_id").alias("total_sessions"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        .filter(F.col("total_sessions") >= 3)
        .orderBy("device_type", "country")
    )
    write_metric(device_country, args.bucket, "time_by_device_country", dt)

    df_pv.unpersist()
    spark.stop()
    print("\nMetricas calculadas")


if __name__ == "__main__":
    main()
