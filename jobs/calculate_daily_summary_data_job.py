from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, to_date
import shutil
import os

spark = SparkSession.builder \
    .appName("Daily summary") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.initialExecutors", "1") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "5") \
    .getOrCreate()

extract_dir = "/opt/data/extracted_drive_data"

df = spark.read.csv(extract_dir, header=True, inferSchema=True)
print(f"Transformation complete. Results saved to  {extract_dir}")

df = df.withColumn("date", to_date(col("date")))

result = df.groupBy("date") \
    .agg(
    count("*").alias("total_count"),
    sum(col("failure").cast("int")).alias("failure_count")
) \
    .orderBy("date")

result.show()

temp_output_dir = "/opt/data/daily-temp"
final_output_dir = "/opt/data/transformed"
final_file_name = "drive_data_daily_summary.csv"

os.makedirs(final_output_dir, exist_ok=True)
os.makedirs(temp_output_dir, exist_ok=True)

result.coalesce(1).write.mode("overwrite").csv(temp_output_dir, header=True)

for file_name in os.listdir(temp_output_dir):
    if file_name.endswith(".csv"):
        shutil.move(os.path.join(temp_output_dir, file_name),
                    os.path.join(final_output_dir, final_file_name))
        break

shutil.rmtree(temp_output_dir)
spark.stop()
