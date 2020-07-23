from pyspark.sql import *

from lib.logger import Log4j

if __name__ == "__main__":
    # SparkSession: Singlenton object
    spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting Hello SparkWithSession")
    #your processing code
    logger.info("Finished HelloSparkWithSession")

    spark.stop()

