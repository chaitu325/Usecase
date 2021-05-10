import configparser
from util import get_spark_session
from read_files import from_files
from transform import *
from to_files import *


def main():

    configread = configparser.ConfigParser()
    configread.read(r'../config/config.ini')

    env = configread.get('env', 'env')
    src_dir = configread.get('src_dir', 'src_dir')
    src_file_format = configread.get('src_file_format', 'src_file_format')
    trg_dir = configread.get('trg_dir', 'trg_dir')
    trg_file_name = configread.get('trg_file_name', 'trg_file_name')
    ref_date = configread.get('ref_date', 'ref_date')

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
    reports_df.show(20)

    # Convert Results to Parquet Format
    to_files_parquet(reports_df, trg_dir, trg_file_name)


if __name__ == '__main__':
    main()
