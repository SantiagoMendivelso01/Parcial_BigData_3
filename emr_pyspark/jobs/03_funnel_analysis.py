"""
03_funnel_analysis.py — Embudo de conversion y productos
spark-submit 03_funnel_analysis.py --date 2024-01-15 --bucket shopstream-XXXX
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark():
    return (
        SparkSession.builder
        .appName("ShopStream-Funnel")
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

    print(f"\nShopStream - Embudo para {args.date}")

    df_pv      = read_clean(spark, args.bucket, "page_view",    dt)
    df_product = read_clean(spark, args.bucket, "product_view", dt)
    df_cart    = read_clean(spark, args.bucket, "cart_event",   dt)

    df_product.cache()
    df_cart.cache()

    # Embudo de conversion
    print("Calculando embudo...")
    n_pv  = df_pv.select("user_id").distinct().count()
    n_prd = df_product.select("user_id").distinct().count()
    n_crt = df_cart.filter(F.col("action") == "add").select("user_id").distinct().count()
    n_chk = df_pv.filter(F.col("page_type") == "checkout").select("user_id").distinct().count()

    data = [
        ("1_page_view",    n_pv,  100.0),
        ("2_product_view", n_prd, round(n_prd / n_pv * 100, 2) if n_pv else 0),
        ("3_cart_add",     n_crt, round(n_crt / n_pv * 100, 2) if n_pv else 0),
        ("4_checkout",     n_chk, round(n_chk / n_pv * 100, 2) if n_pv else 0),
    ]
    funnel = spark.createDataFrame(data, ["funnel_stage", "users", "conversion_rate_pct"])
    funnel.show()
    write_metric(funnel, args.bucket, "conversion_funnel", dt)

    # Productos con alta vista pero baja conversion
    print("Calculando vistas vs carrito...")
    views = (
        df_product
        .groupBy("product_id", "category", "price")
        .agg(F.count("*").alias("total_views"))
    )
    cart_adds = (
        df_cart.filter(F.col("action") == "add")
        .groupBy("product_id")
        .agg(F.count("*").alias("cart_adds"))
    )
    product_conv = (
        views
        .join(cart_adds, "product_id", "left")
        .fillna({"cart_adds": 0})
        .withColumn("view_to_cart_rate", F.round(F.col("cart_adds") / F.col("total_views") * 100, 2))
        .withColumn("is_low_conversion", (F.col("total_views") >= 10) & (F.col("view_to_cart_rate") < 5.0))
        .orderBy(F.col("total_views").desc())
    )
    write_metric(product_conv, args.bucket, "product_view_vs_cart", dt)

    df_product.unpersist()
    df_cart.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
