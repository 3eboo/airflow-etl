import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

POSTGRES_DB = "jdbc:postgresql://postgres/test"
POSTGRES_USER = "test"
POSTGRES_PWD = "postgres"
POSTGRES_DRIVER_JAR = "/spark/resources/jars/postgresql-9.4.1207.jar"


def aggregate_count_per_attributes(group_by_attribute: str, count_attribute: str):
    # Create spark session
    spark = SparkSession.builder.getOrCreate()
    df = (
        spark.read
            .format("jdbc")
            .option("url", POSTGRES_DB)
            .option("dbtable", "processed_users")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PWD)
            .load()
    )

    df = df.groupBy(group_by_attribute).agg(count(count_attribute))

    logging.info(f"Saving total number of {count_attribute} per {group_by_attribute}")

    (
        df.write.format("jdbc")
            .option("url", POSTGRES_DB)
            .option("dbtable", f"{count_attribute}_per_{group_by_attribute}")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PWD)
            .mode("overwrite").save()
    )
    return df


if __name__ == '__main__':
    aggregate_count_per_attributes(sys.argv[1], sys.argv[2])
