import sys
import os
import json
import datacompy
import pandas as pd
from datetime import datetime
from pytz import timezone 
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import when
from pyspark.sql import SparkSession


    sdf_pd = sdf.toPandas()
    tdf_pd = tdf.toPandas()


    comparison = datacompy.Compare(
        sdf_pd, tdf_pd, join_columns=["hash_value"],  # Column(s) to join on
        df1_name="Source Table", df2_name="Target Table"
    )

    print("Comparison completed!")


    timestamp = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d_%H-%M-%S')
    output_dir = os.path.join(os.getcwd(), f"comparison_reports/{timestamp}")
    os.makedirs(output_dir, exist_ok=True)

    comparison_report_path = os.path.join(output_dir, "comparison_summary.txt")
    with open(comparison_report_path, "w") as report_file:
        report_content = comparison.report()
        report_file.write(report_content)

    print(f"Comparison report saved at: {comparison_report_path}")


    comparison.df_diff.to_csv(os.path.join(output_dir, "mismatched_rows.csv"), index=False)
    comparison.df1_unq_rows.to_csv(os.path.join(output_dir, "rows_only_source.csv"), index=False)
    comparison.df2_unq_rows.to_csv(os.path.join(output_dir, "rows_only_target.csv"), index=False)

    print(f"All reports saved to {output_dir}")

except Exception as e:
    print("Exception in main block:", e)
    raise