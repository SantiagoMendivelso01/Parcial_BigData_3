"""
05_anomaly_detection.py — Deteccion de anomalias con z-score e IQR
spark-submit 05_anomaly_detection.py --date 2024-01-15 --bucket shopstream-XXXX
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def create_spark():
    return (
        SparkSession.builder
        .appName("ShopStream-Anomalies")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def zscore_anomalies(df, col_name, threshold=3.0):
    stats = df.agg(
        F.mean(col_name).alias("mean"),
        F.stddev(col_name).alias("std"),
    ).collect()[0]

    mean_val = stats["mean"]
    std_val  = stats["std"]

    if std_val is None or std_val == 0:
        return df.limit(0)

    return (
        df
        .withColumn("z_score", F.abs((F.col(col_name) - F.lit(mean_val)) / F.lit(std_val)).cast(DoubleType()))
        .filter(F.col("z_score") > threshold)
        .withColumn("anomaly_type",  F.lit(f"zscore_{col_name}"))
        .withColumn("anomaly_field", F.lit(col_name))
        .withColumn("anomaly_value", F.col(col_name).cast(DoubleType()))
    )


def iqr_anomalies(df, col_name, factor=3.0):
    quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
    q1, q3    = quantiles[0], quantiles[1]
    iqr       = q3 - q1

    if iqr == 0:
        return df.limit(0)

    lower = q1 - factor * iqr
    upper = q3 + factor * iqr

    return (
        df
        .filter((F.col(col_name) < lower) | (F.col(col_name) > upper))
        .withColumn("z_score", F.abs((F.col(col_name) - F.lit((q1 + q3) / 2)) / F.lit(iqr / 2)).cast(DoubleType()))
        .withColumn("anomaly_type",  F.lit(f"iqr_{col_name}"))
        .withColumn("anomaly_field", F.lit(col_name))
        .withColumn("anomaly_value", F.col(col_name).cast(DoubleType()))
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",   required=True)
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    dt    = datetime.strptime(args.date, "%Y-%m-%d")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\nShopStream - Anomalias para {args.date}")

    pv_path = f"s3://{args.bucket}/processed/clean/page_view/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    cl_path = f"s3://{args.bucket}/processed/clean/click/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"

    df_pv    = spark.read.parquet(pv_path)
    df_click = spark.read.parquet(cl_path)

    # Anomalias en tiempo de pagina
    time_anom = (
        zscore_anomalies(df_pv, "time_on_page_seconds")
        .select("session_id", "user_id", "page_url", "page_type",
                "z_score", "anomaly_type", "anomaly_field", "anomaly_value")
    )

    # Sesiones con demasiados clicks
    click_per_session = (
        df_click
        .groupBy("session_id", "user_id")
        .agg(F.count("*").alias("click_count"))
    )
    click_anom = (
        iqr_anomalies(click_per_session, "click_count")
        .select("session_id", "user_id",
                F.lit(None).alias("page_url"),
                F.lit(None).alias("page_type"),
                "z_score", "anomaly_type", "anomaly_field", "anomaly_value")
    )

    all_anomalies = (
        time_anom.unionByName(click_anom)
        .withColumn("event_date",  F.lit(args.date))
        .withColumn("detected_at", F.current_timestamp())
        .dropDuplicates(["session_id", "anomaly_type"])
    )

    total = all_anomalies.count()
    print(f"Total anomalias: {total:,}")
    all_anomalies.groupBy("anomaly_type").count().show()

    out = f"s3://{args.bucket}/processed/metrics/anomalies/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    all_anomalies.write.mode("overwrite").parquet(out)
    print(f"OK anomalies exportadas -> {out}")

    spark.stop()


if __name__ == "__main__":
    main()
