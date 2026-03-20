from pyspark.sql.functions import rand, expr, current_timestamp

df = (
    spark.range(0, 10000000)
    .withColumnRenamed("id", "sensor_id")
    .withColumn("temperature", rand()*100)
    .withColumn("pressure", rand()*500)
    .withColumn("flow_rate", rand()*1000)
    .withColumn("well_id", expr("sensor_id % 1000"))
    .withColumn("timestamp", current_timestamp())
)

df.write.format("delta").mode("overwrite").save("/mnt/bronze/sensor/")