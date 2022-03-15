import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col
from pyspark.sql.types import IntegerType, StructType, StructField, StringType

POSTGRES_DRIVER_JAR = "/spark/resources/jars/postgresql-9.4.1207.jar"
POSTGRES_DB = "jdbc:postgresql://postgres/test"
POSTGRES_USER = "test"
POSTGRES_PWD = "postgres"


def extract_load_dataframe(csv_file: str):
    # Create spark session
    spark = SparkSession.builder.getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    ####################################
    # Parameters
    ####################################

    table_schema = StructType([StructField('user_id', IntegerType()),
                               StructField('regionid', StringType()),
                               StructField('duration', IntegerType()),
                               StructField('channel', StringType()),
                               StructField('unique_sess_id', StringType()),
                               StructField('app_id', IntegerType()),
                               StructField('device', StringType()),
                               StructField('cats', StringType()),
                               StructField('email', StringType()),
                               StructField('backend_id', IntegerType())])

    #######################################
    # load csv to spark dataframe
    #######################################

    df = spark.read.format("csv").option("header", True).schema(table_schema).load(csv_file)

    df = df.withColumn('backend_id', regexp_extract(col('email'), r'-([0-9]+)@zattoo.com', 1))

    logging.info(f"Saving dataframe into 'processed_users' table {df.show()}")
    (
        df.write.format("jdbc").option("url", POSTGRES_DB).option("dbtable", "processed_users")
            .option("user", POSTGRES_USER).option("password", POSTGRES_PWD).mode("overwrite").save()
    )
    return df


if __name__ == '__main__':
    extract_load_dataframe(sys.argv[1])
