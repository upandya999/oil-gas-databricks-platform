from pyspark.sql.functions import avg

df = spark.read.format("delta").load("/mnt/silver/")
df.groupBy("sensor_id").agg(avg("temperature")) \
  .write.format("delta").mode("overwrite").save("/mnt/gold/")