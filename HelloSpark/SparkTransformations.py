import sys

from pyspark.sql import *

from lib.logger import Log4j
from lib.utils import get_spark_app_config, load_survey_df

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usaage: HelloSpark: <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")

    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())

    """
    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(sys.argv[1])
    """
    survey_df = load_survey_df(spark, sys.argv[1])
    # Transformations
    filtered_survey_df = survey_df.where("Age < 40").select("Age", "Gender", "Country", "state")
    grouped_df = filtered_survey_df.groupBy("Country")
    count_df = grouped_df.count()
    count_df.show()

    logger.info("Finished HelloSpark")









