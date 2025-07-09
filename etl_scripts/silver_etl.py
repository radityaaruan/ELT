import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import to_date

# Set JAVA_HOME (penting untuk Mac Intel)
os.environ["JAVA_HOME"] = "/usr/local/opt/openjdk@11"

# Inisialisasi Spark
import findspark
findspark.init()

spark = SparkSession.builder.appName("Silver_ETL_Penjualan").getOrCreate()

# Definisikan schema untuk memastikan tipe data sesuai (biar siap ke data warehouse)
schema = StructType([
    StructField("id_transaksi", StringType(), True),
    StructField("id_produk", StringType(), True),
    StructField("jumlah", FloatType(), True),
    StructField("harga_satuan", FloatType(), True),
    StructField("tanggal_transaksi", StringType(), True)  # sementara masih string
])

# Load data mentah dengan schema yang sudah ditentukan
df_raw = spark.read.option("header", True).schema(schema).csv("../data/penjualan_2025-07-03.csv")

# Convert tanggal_transaksi jadi format date (YYYY-MM-DD)
df_silver = df_raw.withColumn("tanggal_transaksi", to_date("tanggal_transaksi", "dd-MM-yyyy"))

# Preview hasil silver
#df_silver.printSchema()
df_silver.show()

# Simpan hasil silver ke file Parquet
# (Parquet lebih efisien untuk data warehouse)
df_silver.write.mode("overwrite").parquet("../silver_output.parquet")

