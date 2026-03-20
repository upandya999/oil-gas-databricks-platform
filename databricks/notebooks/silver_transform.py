from pyspark.sql.functions import col

df = spark.read.format("delta").load("/mnt/bronze/processed/")
df.filter(col("temperature").isNotNull()) \
  .write.format("delta").mode("overwrite").save("/mnt/silver/")