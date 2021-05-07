from pyspark.sql.functions import *


# Dropping the duplicates from the dataframe
def drop_duplicates(df):
    df = df.dropDuplicates()
    return df


# Converting String to Timestamp
def string_timestamp(df):
    df = df.withColumn("UP_EVENT_DATE", to_timestamp("EVENT_DATE", "dd/MM/yyyy H:mm")).drop("EVENT_DATE")
    return df


# Extracting Date_Only
def extract_date(df):
    df = df.withColumn("Date_Only", to_date("UP_EVENT_DATE", "yyyy-MM-dd"))
    return df


# Joining to Data Frames
def broadcast_join(df1, df2):
    df = df1.join(broadcast(df2), df1.WEB_PAGEID == df2.WEB_PAGEID, "inner").drop(df2.WEB_PAGEID)
    return df


# Spark SQL to perform frequency and duration
def df_sql(spark, df, refdate):
    df.createOrReplaceTempView("WebDetailsTable")
    df_a = spark.sql(f"""
        Select USER_ID,
        CASE WHEN first(Date_Only) >= DATE_SUB('{refdate}', 365)
                     AND first(WEBPAGE_TYPE) == 'movies' THEN 
                    DATEDIFF('{refdate}',MAX(Date_Only)) ELSE NULL END 
        AS pageview_movies_dur_365,
        CASE WHEN first(Date_Only) >= DATE_SUB('{refdate}', 365)
                     AND first(WEBPAGE_TYPE) == 'news' THEN 
                    DATEDIFF('{refdate}',MAX(Date_Only)) ELSE NULL END 
        AS pageview_news_dur_365,            
        COUNT(
        CASE WHEN Date_Only >= DATE_SUB('{refdate}', 365)
                     AND WEBPAGE_TYPE = 'movies'
               THEN 1 ELSE NULL END
        ) AS pageview_movies_fre_365,
        COUNT(
        CASE WHEN Date_Only >= DATE_SUB('{refdate}', 365)
                     AND WEBPAGE_TYPE = 'news'
               THEN 1 ELSE NULL END
        ) AS pageview_news_fre_365,
        COUNT(
        CASE WHEN Date_Only >= DATE_SUB('{refdate}', 730)
                     AND WEBPAGE_TYPE = 'movies'
               THEN 1 ELSE NULL END
        ) AS pageview_movies_fre_730,
        COUNT(
        CASE WHEN Date_Only >= DATE_SUB('{refdate}', 730)
                     AND WEBPAGE_TYPE = 'news'
               THEN 1 ELSE NULL END
        ) AS pageview_news_fre_730,
        COUNT(
        CASE WHEN Date_Only >= DATE_SUB('{refdate}', 1460)
                     AND WEBPAGE_TYPE = 'movies'
               THEN 1 ELSE NULL END
        ) AS pageview_movies_fre_1460,
        COUNT(
        CASE WHEN Date_Only >= DATE_SUB('{refdate}', 1460)
                     AND WEBPAGE_TYPE = 'news'
               THEN 1 ELSE NULL END
        ) AS pageview_news_fre_1460,       
        COUNT(
        CASE WHEN Date_Only >= DATE_SUB('{refdate}',2920)
                     AND WEBPAGE_TYPE = 'movies'
               THEN 1 ELSE NULL END
        ) AS pageview_movies_fre_2920,
        COUNT(
        CASE WHEN Date_Only >= DATE_SUB('{refdate}',2920)
                     AND WEBPAGE_TYPE = 'news'
               THEN 1 ELSE NULL END
        ) AS pageview_news_fre_2920
        FROM
           WebDetailsTable
        GROUP BY
           USER_ID
        ORDER BY 
            USER_ID
           """)
    return df_a
