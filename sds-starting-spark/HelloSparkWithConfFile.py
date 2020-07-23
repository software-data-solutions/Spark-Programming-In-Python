from pyspark.sql import *

from lib.logger import Log4j
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    conf = get_spark_app_config()
    # SparkSession: Singlenton object
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting HelloSparkWithConfigFile")
    # your processing code
    conf_out = spark.sparkContext.getConf()
    logger.info (conf_out.toDebugString())

    logger.info("Finished HelloSparkWithConfigFile")

    #spark.stop()

