import datetime as dt
import json
import logging
from io import BytesIO
from confluent_kafka import Consumer, KafkaError
from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Configuration des logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connexion à MinIO
minio_client = Minio(
    "localhost:9000",  # Adapte l'URL selon ton setup
    access_key="1KvQ0sufscIQ0bcEfd2K",  # Utilise tes propres identifiants
    secret_key="zrpfHksSBAPS3vYwCcVwg08m2nmB0XU29i4YE1PR",
    secure=False
)

# Fonction pour consommer des messages Kafka et les enregistrer dans MinIO
def consume_and_store():
    # Configuration du consommateur Kafka
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    })

    # Abonne-toi au topic Kafka
    consumer.subscribe(['transaction'])

    try:
        while True:
            # Consommer un message de Kafka
            msg = consumer.poll(1.0)  # Attendre 1 seconde

            if msg is None:  # Aucune donnée disponible
                continue
            if msg.error():  # Si une erreur se produit avec le message
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Fin de partition atteinte {msg.topic()} [{msg.partition()}] @ {msg.offset()}")
                else:
                    logger.error(f"Erreur de message Kafka : {msg.error()}")
                continue

            # Convertir le message en dictionnaire
            transaction = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Transaction reçue : {transaction}")

            # Stocker la transaction dans MinIO (par exemple, dans un fichier JSON)
            object_name = f"transaction_{transaction['id_transaction']}.json"
            data = json.dumps(transaction)

            # Utiliser BytesIO pour simuler un fichier
            data_io = BytesIO(data.encode('utf-8'))

            minio_client.put_object(
                "transactions-bucket",  # Assure-toi que ce bucket existe dans MinIO
                object_name,
                data_io,
                len(data)
            )
            logger.info(f"Transaction sauvegardée dans MinIO sous {object_name}")

    except KeyboardInterrupt:
        logger.info("Interruption par l'utilisateur")
    finally:
        consumer.close()


# Fonction pour transformer les données avec Spark
def transform_data_with_spark():
    # Initialisation de la session Spark
    spark = SparkSession.builder \
        .appName("MinIODataProcessing") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "1KvQ0sufscIQ0bcEfd2K") \
        .config("spark.hadoop.fs.s3a.secret.key", "zrpfHksSBAPS3vYwCcVwg08m2nmB0XU29i4YE1PR") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Définir le schéma des données JSON
    schema = StructType([
        StructField("id_transaction", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Lire les données JSON depuis MinIO
    df = spark.read.json("s3a://transactions-bucket/transaction_*.json", schema=schema)

    # Afficher le schéma et les données
    logger.info("Schéma des données :")
    df.printSchema()
    logger.info("Données brutes :")
    df.show()

    # Convertir le timestamp en format de date
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))

    # Filtrer les transactions avec un montant supérieur à 100
    df_filtered = df.filter(col("amount") > 100)

    # Afficher les données transformées
    logger.info("Données transformées :")
    df_filtered.show()

    # Sauvegarder les données transformées dans un nouveau bucket MinIO
    df_filtered.write.json("s3a://transformed-transactions-bucket/")
    logger.info("Données transformées sauvegardées dans MinIO sous s3a://transformed-transactions-bucket/")

    # Arrêter la session Spark
    spark.stop()


if __name__ == "__main__":
    # Étape 1: Consommer les messages Kafka et les stocker dans MinIO
    consume_and_store()

    # Étape 2: Transformer les données avec Spark
    transform_data_with_spark()