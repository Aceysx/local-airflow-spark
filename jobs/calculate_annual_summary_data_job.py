from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, to_date,year,when,element_at,split
import shutil
import os

spark = (SparkSession.builder.appName("Annual summary")
         .config("spark.executor.memory", "4g")
         .config("spark.executor.cores", "2")
         .config("spark.default.parallelism", "100")
         .getOrCreate())

extract_dir = "/opt/data/extracted_drive_data"

df = spark.read.csv(extract_dir, header=True, inferSchema=True)

result = df.withColumn("year", year("date")) \
    .withColumn("brand", element_at(split("model", " "), 1)) \
    .filter(element_at(split("model", " "), 2).isNotNull()) \
    .groupBy("year", "brand") \
    .agg(count(when(df.failure == 1, True)).alias("failures")) \
    .orderBy("year", "brand")

result.show()

temp_output_dir = "/opt/data/temp"
final_output_dir = "/opt/data/transformed"
final_file_name = "drive_data_annual_summary.csv"

# 确保输出目录存在
os.makedirs(final_output_dir, exist_ok=True)
os.makedirs(temp_output_dir, exist_ok=True)

# 写入到临时目录
result.coalesce(1).write.mode("overwrite").csv(temp_output_dir, header=True)

# 找到生成的 CSV 文件并移动/重命名
for file_name in os.listdir(temp_output_dir):
    if file_name.endswith(".csv"):
        shutil.move(os.path.join(temp_output_dir, file_name),
                    os.path.join(final_output_dir, final_file_name))
        break

# 删除临时目录
shutil.rmtree(temp_output_dir)
# 关闭 SparkSession
spark.stop()
