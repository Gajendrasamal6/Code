import sys
import io
import json
import datacompy
import pandas as pd
from datetime import datetime
from pytz import timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import when, lit, col


spark = SparkSession.builder.appName("OracleTableComparison").getOrCreate()

def read_oracle_table(table_schema, table_name, user, password, host, db_name):
    try:
        table = f"{table_schema}.{table_name}"
        print(f"Reading table: {table}")
        oracle_df = spark.read.format("jdbc") \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .option("url", f"jdbc:oracle:thin:@//{host}:1521/{db_name}") \
            .option("dbtable", f"(SELECT * FROM {table})") \
            .option("user", user) \
            .option("password", password) \
            .load()
        oracle_df.show()
        print(f"Successfully read data from {table}")
        return oracle_df
    except Exception as e:
        print(f"Oracle Connection Error: {e}")
        return None

def save_dataframe_to_csv(df, file_path):
    if df is not None:
        df.toPandas().to_csv(file_path, header=True, index=False)
        print(f"Saved file: {file_path}")

try:
    print("Starting Comparison Job")
    

    oracle_user = "username"
    oracle_password = "password"
    oracle_host = "host"
    oracle_db = "db_name"
    
    source_df = read_oracle_table("schema", "source_table", oracle_user, oracle_password, oracle_host, oracle_db)
    target_df = read_oracle_table("schema", "target_table", oracle_user, oracle_password, oracle_host, oracle_db)
    
    if source_df is None or target_df is None:
        raise Exception("Failed to read tables. Aborting.")
    

    sdf = source_df.select([col(x).alias(x.lower()) for x in source_df.columns])
    tdf = target_df.select([col(x).alias(x.lower()) for x in target_df.columns])


    for col_name in sdf.columns:
        sdf = sdf.withColumn(col_name, when(sdf[col_name] == '', None).otherwise(sdf[col_name]))
    for col_name in tdf.columns:
        tdf = tdf.withColumn(col_name, when(tdf[col_name] == '', None).otherwise(tdf[col_name]))
    

    sdf = sdf.withColumn("hash_value", F.sha2(F.concat_ws("", *(col(col_name).cast("string") for col_name in sdf.columns)), 256))
    tdf = tdf.withColumn("hash_value", F.sha2(F.concat_ws("", *(col(col_name).cast("string") for col_name in sdf.columns)), 256))

    print("Data Preprocessing Completed")
    

    comparison = datacompy.SparkCompare(spark, sdf, tdf, join_columns=[("hash_value", "hash_value")], cache_intermediates=True)
    print("Comparison Completed!")
    
    current_date = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d_%H-%M-%S')
    output_folder = f"comparison_reports/{current_date}"
    os.makedirs(output_folder, exist_ok=True)

    count_summary_path = f"{output_folder}/count_summary.txt"
    comparison_summary_path = f"{output_folder}/comparison_summary.txt"
    rows_only_source_path = f"{output_folder}/rows_only_source.csv"
    rows_only_target_path = f"{output_folder}/rows_only_target.csv"
    mismatched_rows_path = f"{output_folder}/mismatched_rows.csv"

    mismatch_df = comparison.rows_both_mismatch
    source_only_df = comparison.rows_only_base
    target_only_df = comparison.rows_only_compare

    summary = f"""
    Source Table Count: {sdf.count()}
    Target Table Count: {tdf.count()}
    Mismatching Rows: {mismatch_df.count()}
    Rows Only in Source: {source_only_df.count()}
    Rows Only in Target: {target_only_df.count()}
    """

    with open(count_summary_path, "w") as f:
        f.write(summary)
    print("Count summary saved")

    save_dataframe_to_csv(mismatch_df, mismatched_rows_path)
    save_dataframe_to_csv(source_only_df, rows_only_source_path)
    save_dataframe_to_csv(target_only_df, rows_only_target_path)


    with io.StringIO() as report_file:
        comparison.report(file=report_file)
        report_file_content = report_file.getvalue()
        report_file_content = report_file_content.replace("base", "Source Oracle Table").replace("compare", "Target Oracle Table")
        with open(comparison_summary_path, "w") as f:
            f.write(report_file_content)
    print("Comparison report saved")

except Exception as e:
    print(f"Error: {e}")

print("Job Completed!")
