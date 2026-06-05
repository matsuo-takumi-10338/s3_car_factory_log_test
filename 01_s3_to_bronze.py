# 変数の定義（松尾さんの環境用）
env = "dev"
s3_source_path = "s3://databricks-sample-bucket-20260602/car_factory_dev/raw/"
schema_location = "s3://databricks-sample-bucket-20260602/car_factory_dev/checkpoints/bronze/_schema_metadata"
checkpoint_location = "s3://databricks-sample-bucket-20260602/car_factory_dev/checkpoints/bronze"
bronze_data_path = f"s3://databricks-sample-bucket-20260602/car_factory_{env}/database/bronze/car_factory_bronze"

# 読み込み側
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.partitionColumns", "factory_id")
    .option("header", "true")
    .load(s3_source_path)
)

# 書き込み側
query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .option("path", bronze_data_path)
    .trigger(availableNow=True)
    .toTable(f"car_factory_{env}.bronze.car_factory_bronze")
)