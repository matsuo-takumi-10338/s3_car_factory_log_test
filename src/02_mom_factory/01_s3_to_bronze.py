import dlt
from pyspark.sql import functions as F
from src.modules.env_params import EnvParams


env_params = EnvParams(domain="mom_factory", layer="bronze")
table_name = "cf_mom_factory_dev.bronze.bronze_mom_factory"

spark.conf.set("pipelines.metastore.checkpoint.bronze", env_params.get_path("checkpoint_location"))      


@dlt.table(
    name=table_name,
    comment="ブロンズ-車両製造実績"
)
def brz_mom_factory():
    
    raw_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", env_params.get_path("schema_location"))
        .option("header", "true")
        .load(env_params.get_path("s3_source_path"))
    )
    
    return (raw_df
        .withColumn("_input_file_path", F.col("_metadata.file_path"))      
        .withColumn("_processed_timestamp", F.current_timestamp()) 
    )