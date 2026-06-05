from env_config import EnvConfig

config = EnvConfig(layer="bronze")

# 読み込み側
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", config.get_env_params("schema_location"))
    .option("cloudFiles.partitionColumns", "factory_id")
    .option("header", "true")
    .load(config.get_env_params("s3_source_path"))
)

# 書き込み側
query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", config.get_env_params("checkpoint_location"))
    .option("mergeSchema", "true")
    .option("path", config.get_env_params("data_path"))
    .trigger(availableNow=True)
    .toTable(config.get_env_params("table_name"))
)