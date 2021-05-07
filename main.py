import os
from util import get_spark_session
from read_files import from_files
from transform import *
from to_files import *


def main():
    env = os.environ.get('ENVIRON')
    src_dir = os.environ.get('SRC_DIR')
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    trg_dir = os.environ.get('TRG_DIR')
    trg_file_name = os.environ.get('TRG_FILE_NAME')
    ref_date = os.environ.get('REF_DATE')

    # Creating Spark Session and Extracting the Files
    spark = get_spark_session(env, 'UC')
    df_fact = from_files(spark, src_dir, 'fact', src_file_format)
    df_lookup = from_files(spark, src_dir, 'lookup', src_file_format)

    # Data Cleaning Removing Duplicates
    df_fact_c = string_timestamp(df_fact)
    df_fact_c = drop_duplicates(df_fact_c)
    df_lookup_cl = drop_duplicates(df_lookup)
    df_fact_cl = extract_date(df_fact_c)

    # Data Manipulations

    # Joining two dataframes
    df = broadcast_join(df_fact_cl, df_lookup_cl)

    # Performing SPARK SQL Queries for frequency
    reports_df = df_sql(spark, df, ref_date)
    reports_df.printSchema()
    reports_df.show()

    # Convert Results to Parquet Format

    to_files_parquet(reports_df, trg_dir, trg_file_name)


if __name__ == '__main__':
    main()
