def from_files(spark, data_dir, file_name, file_format):
    df = spark.read. \
        format(file_format). \
        load(f'{data_dir}/{file_name}.{file_format}', header="True", inferSchema="True")
    return df