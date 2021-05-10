# To write files to parquet format.
def to_files_parquet(df, tg_dir, tg_file_pattern):
    df.write. \
        mode('append'). \
        parquet(f'{tg_dir}/{tg_file_pattern}')

