import logging
import pytest
import datetime
from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal
from src.main.python.transform import *


def suppress_py4j_logging():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("test") \
        .getOrCreate()


def test_drop_duplicates(spark):
    src_dt = [(111, '2019-01-31', 'movies'),
              (111, '2019-01-31', 'movies'),
              (111, '2019-01-01', 'movies'),
              (111, '2018-01-01', 'news'),
              (121, '2019-02-01', 'movies'),
              (131, '2019-01-15', 'movies')
              ]
    src_df = spark.createDataFrame(src_dt, ["USER_ID", "Date_Only", "WEBPAGE_TYPE"])

    act_df = drop_duplicates(src_df)

    exp_dt = [(111, '2019-01-31', 'movies'),
              (111, '2019-01-01', 'movies'),
              (111, '2018-01-01', 'news'),
              (121, '2019-02-01', 'movies'),
              (131, '2019-01-15', 'movies')]
    exp_df = spark.createDataFrame(exp_dt, ["USER_ID", "Date_Only", "WEBPAGE_TYPE"])
    act_df = act_df.orderBy('USER_ID', 'Date_Only')
    exp_df = exp_df.orderBy('USER_ID', 'Date_Only')

    assert_pyspark_df_equal(act_df, exp_df)


def test_string_timestamp(spark):
    src_dt = [(111, '31/01/2019 15:43', 'movies'),
              ]
    src_df = spark.createDataFrame(src_dt, ["USER_ID", "EVENT_DATE", "WEBPAGE_TYPE"])

    act_df = string_timestamp(src_df)

    exp_dt = [(111, 'movies', datetime.datetime(2019, 1, 31, 15, 43, 0))]
    exp_df = spark.createDataFrame(exp_dt, ["USER_ID", "WEBPAGE_TYPE", "UP_EVENT_DATE"])
    act_df.show()
    exp_df.show()

    assert_pyspark_df_equal(act_df, exp_df)


def test_df_sql(spark):
    src_dt = [(111, datetime.date(2019, 1, 31), 'movies'),
              (111, datetime.date(2019, 1, 1), 'movies'),
              (111, datetime.date(2018, 1, 1), 'news'),
              (121, datetime.date(2019, 2, 1), 'movies'),
              (111, datetime.date(2018, 2, 1), 'news'),
              (121, datetime.date(2019, 1, 29), 'movies'),
              (111, datetime.date(2019, 2, 1), 'news'),
              (131, datetime.date(2019, 1, 15), 'movies'),
              (111, datetime.date(2018, 1, 12), 'news'),
              (131, datetime.date(2018, 1, 1), 'movies'),
              (121, datetime.date(2018, 1, 1), 'news'),
              (111, datetime.date(2017, 2, 1), 'movies')
              ]
    src_df = spark.createDataFrame(src_dt, ["USER_ID", "Date_Only", "WEBPAGE_TYPE"])

    act_df = df_sql(spark, src_df, '2019-02-02')

    exp_dt = [(111, 1, 2), (121, 1, 2), (131, 18, 1)]
    exp_df = spark.createDataFrame(exp_dt, ["USER_ID", "pageview_movies_dur_365", "pageview_movies_fre_365"])
    act_df_t = act_df.select("USER_ID", act_df.pageview_movies_dur_365.cast('BIGINT'), "pageview_movies_fre_365")
    act_df.show()
    exp_df.show()

    assert_pyspark_df_equal(act_df_t, exp_df)