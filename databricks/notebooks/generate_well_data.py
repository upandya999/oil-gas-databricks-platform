from pyspark.sql.functions import expr

df = spark.range(0,1000).withColumnRenamed("id","well_id") \
    .withColumn("status", expr("CASE WHEN rand()>0.5 THEN 'active' ELSE 'inactive' END"))

df.write.format("delta").mode("overwrite").save("/mnt/bronze/well/")