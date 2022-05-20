import logging
import sys

from pyspark.sql import SparkSession

POSTGRES_DB = "jdbc:postgresql://postgres/test"
POSTGRES_USER = "test"
POSTGRES_PWD = "postgres"
POSTGRES_DRIVER_JAR = "/spark/resources/jars/postgresql-9.4.1207.jar"


def aggregate_sum_per_attributes(group_by_attribute: str, sum_attribute: str):
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

    df = df.groupBy(group_by_attribute).sum(sum_attribute)

    logging.info(df.show())
    logging.info(f"Saving total {sum_attribute} per {group_by_attribute}")

    (
        df.write.format("jdbc").option("url", POSTGRES_DB)
            .option("dbtable", f"{sum_attribute}_per_{group_by_attribute}")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PWD)
            .mode("overwrite").save()
    )
    return df


if __name__ == '__main__':
    aggregate_sum_per_attributes(sys.argv[1], sys.argv[2])
