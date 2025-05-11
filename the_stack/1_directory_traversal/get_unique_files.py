import argparse
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main(spark, args):
    files_df = spark.read.parquet(args.input_path)
    files_df = (
        files_df.select("content_id", "blob_id", "length", "path")
        .withColumn("filename", F.element_at(F.split("path", "/"), -1))
        .drop("path")
    )
    files_df = files_df.groupBy("content_id", "blob_id", "length").agg(
        F.collect_set("filename").alias("filenames")
    )

    files_df = files_df.withColumn("blob_prefix", F.substring("blob_id", 1, 2))

    files_df.repartition(128).write.format("json").option("compression", "gzip").mode(
        "overwrite"
    ).partitionBy("blob_prefix").save(args.output_path)


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument(
        "--input_path",
        default="./output/file_paths/",
        type=str,
        help="Local path to the StackV2 file path data",
    )
    args.add_argument(
        "--output_path",
        default="./output/files_to_download/",
        type=str,
        help="Local path to the unique files dataset",
    )
    args = args.parse_args()

    # Get Spark master URL from environment variable or use local
    spark_master = os.environ.get("SPARK_MASTER_URL", "local[*]")

    spark = (
        SparkSession.builder
        .config("spark.sql.shuffle.partitions", 2048)
        .config("spark.master", spark_master)
        .appName("find_file_paths")
        .getOrCreate()
    )

    main(spark, args)
