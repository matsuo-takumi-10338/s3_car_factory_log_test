# ==========================================
# ⑨ DWH内の個別連携用テーブル（ブロンズ層の構築）
# ==========================================

# --- ❶ 勉強用に環境変数・アカウントIDをハードコーディング（動的組み立て） ---
env = "dev"  # テスト用に一旦 dev で固定
aws_account_id = "578690312429"  # 松尾さんのAWSアカウントID
bucket_name = "databricks-sample-bucket-20260602"  # 対象のS3バケット名

# --- ❷ パスとSQSの動的組み立て ---
s3_source_path  = f"s3://{bucket_name}/car_factory_{env}/raw/"
checkpoint_base = f"s3://{bucket_name}/car_factory_{env}/checkpoints/bronze/"
checkpoint_path = checkpoint_base + "_stream_checkpoints"
schema_path     = checkpoint_base + "_schema_metadata"

# 松尾さんが自前構築したSQSのURLを美しく自動組み立て
sqs_url = f"https://sqs.ap-northeast-1.amazonaws.com/{aws_account_id}/car-factory-raw-data-sqs"

# --- ❸ パイプライン本体（Auto Loaderによる自動吸い込み） ---
print(f"[INFO] Auto Loaderを起動します。対象パス: {s3_source_path}")
print(f"[INFO] 監視用自前SQS: {sqs_url}")

bronze_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    # ★重要仕様：CSVに工場IDはないため、型推論を false にして一律Stringで安全に受け止める
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("header", "true")
    # ★自前SQS連携設定
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.queueUrl", sqs_url)
    .load(s3_source_path))

# --- ❹ デルタテーブル（Managed Table）への書き込み ---
query = (bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    # ★本番バッチ運用・テストに最適な trigger(availableNow=True)
    .trigger(availableNow=True)
    # カタログ名：car_factory_dev ➔ スキーマ名：bronze ➔ テーブル名：car_factory_bronze
    .toTable(f"car_factory_{env}.bronze.car_factory_bronze"))

# ストリームの完了を待機してログを出力
query.awaitTermination()
print("[INFO] ブロンズ層への吸い込みが正常に完了しました")