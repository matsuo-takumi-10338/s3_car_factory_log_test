s3_source_path = "s3://databricks-sample-bucket-20260602/raw/"

checkpoint_path = "s3://databricks-sample-bucket-20260602/checkpoints/car_factory_bronze/"


bronze_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(s3_source_path))


query = (bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True) 
    .toTable("car_factory_dev.bronze.car_factory_bronze")) 