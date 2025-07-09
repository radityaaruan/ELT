from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Inisialisasi SparkSession + Load JDBC Driver (.jar)
spark = SparkSession.builder \
    .appName("Gold_ETL_Penjualan") \
    .config("spark.jars", "../config/postgresql-42.2.20.jar") \
    .getOrCreate()

# Load data dari Bronze Layer
df_bronze = spark.read.parquet("../bronze_output.parquet")

# Tambahkan kolom inserted_at (timestamp saat insert ke PostgreSQL)
df_gold = df_bronze.withColumn("inserted_at", current_timestamp())

# Susun ulang kolom agar rapih
df_gold = df_gold.select(
    "id_transaksi",
    "id_produk",
    "jumlah",
    "harga_satuan",
    "total_harga",
    "tanggal_transaksi",
    "inserted_at"
)

# Simpan ke PostgreSQL (auto-create table jika belum ada)
df_gold.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/etl_pegadaian") \
    .option("dbtable", "gold.penjualan") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# Preview hasilnya
df_gold.show()
