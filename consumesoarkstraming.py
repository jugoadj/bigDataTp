from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Configuration Spark
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .getOrCreate()

# Schéma des données
schema = StructType([
    StructField("id_transaction", StringType(), True),
    StructField("type_transaction", StringType(), True),
    StructField("montant", DoubleType(), True),
    StructField("devise", StringType(), True),
    StructField("date", StringType(), True),
    StructField("lieu", StringType(), True),
    StructField("moyen_paiement", StringType(), True),
    StructField("details", StructType([
        StructField("produit", StringType(), True),
        StructField("quantite", DoubleType(), True),
        StructField("prix_unitaire", DoubleType(), True)
    ]), True),
    StructField("utilisateur", StructType([
        StructField("id_utilisateur", StringType(), True),
        StructField("nom", StringType(), True),
        StructField("adresse", StringType(), True),
        StructField("email", StringType(), True)
    ]), True)
])

# Lire les données de Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transaction") \
    .option("startingOffsets", "latest") \
    .load()

# Désérialiser les données JSON
df_json = df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data"))

# Filtrer les données non valides
df_filtered = df_json.filter(col("data").isNotNull()).select("data.*")

# Transformations
df_transformed = df_filtered \
    .withColumn("montant_eur", col("montant") * 0.85) \
    .withColumn("date_timestamp", from_utc_timestamp(col("date"), "UTC")) \
    .withColumn("date", to_date(col("date_timestamp"))) \
    .filter(col("moyen_paiement") != "erreur") \
    .filter(col("lieu").isNotNull())

# Configuration MinIO
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9001")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio123")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

# Écrire les données transformées en streaming sur MinIO
query = df_transformed.writeStream \
    .format("parquet") \
    .option("path", "s3a://transactions-bucket/transactions.parquet") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .outputMode("append") \
    .start()

# Attendre la fin du streaming
query.awaitTermination()