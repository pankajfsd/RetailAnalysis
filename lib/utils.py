from pyspark.sql import SparkSession
from lib.configReader import get_spark_config

# create spark session
def get_spark_session(env):
    if env=="LOCAL":
        return SparkSession.builder \
        .config(conf=get_spark_config(env)) \
        .config('spark.driver.extraJavaOptions','-Dlog4j.configuration=file:log4j.properties') \
        .master("local[2]") \
        .getOrCreate()
    else:
        return SparkSession.builder \
        .config(conf=get_spark_config(env)) \
        .config('spark.driver.extraJavaOptions','-Dlog4j.configuration=file:log4j.properties') \
        .enableHiveSupport() \
        .getOrCreate()
        