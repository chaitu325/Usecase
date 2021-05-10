from pyspark.sql import SparkSession

def get_spark_session(env, app_name):
    if env == 'DEV':
        spark = SparkSession. \
            builder. \
            master('local'). \
            appName('UC'). \
            getOrCreate()
        return spark
    elif env == 'PROD':
        spark = SparkSession. \
            builder. \
            master('yarn'). \
            appName('UC'). \
            getOrCreate()
        return spark
    else:
        return null