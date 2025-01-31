import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

# Configuration Spark
spark = SparkSession.builder \
    .appName("ReadFromMinIO") \
    .getOrCreate()

# Configuration MinIO
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9001")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio123")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

# Lire les données Parquet
df = spark.read.parquet("s3a://warehouse/transactions")
df.show()


# Convertir le DataFrame en Pandas
df_pandas = df.toPandas()

# Tracer un graphique
plt.bar(df_pandas["type_transaction"], df_pandas["montant_eur"])
plt.xlabel("Type de transaction")
plt.ylabel("Montant en EUR")
plt.title("Montant des transactions par type")
plt.show()