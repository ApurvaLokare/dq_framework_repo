from pyspark.sql import SparkSession

from common.constants import DQ_BUCKET
from common.custom_logger import get_logger


def createSparkSession():
    logger = get_logger()
    spark = (
        SparkSession.builder.appName("IcebergTableReader")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            "spark.sql.catalog.s3tablesbucket",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.s3tablesbucket.catalog-impl",
            "software.amazon.s3tables.iceberg.S3TablesCatalog"
        )
        .config("spark.sql.catalog.s3tablesbucket.warehouse", DQ_BUCKET)
        .getOrCreate()
    )
    logger.info("[CREATE_SPARK_SESSION] Created spark session with required "
                "s3tables configuration.")
    return spark
