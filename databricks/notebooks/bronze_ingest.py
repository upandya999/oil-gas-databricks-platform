df = spark.read.format("delta").load("/mnt/bronze/sensor/")
df.write.format("delta").mode("append").save("/mnt/bronze/processed/")