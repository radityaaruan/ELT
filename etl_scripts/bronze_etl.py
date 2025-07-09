from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
from pyspark.sql.types import IntegerType

# Inisialisasi SparkSession
spark = SparkSession.builder.appName("Bronze_ETL_Penjualan").getOrCreate()

# Baca parquet hasil silver
df_silver = spark.read.parquet("../silver_output.parquet")

# Transformasi Bronze dari df_silver
df_bronze = (
    df_silver
    # Seragamkan id_produk jadi huruf kapital
    .withColumn("id_produk", upper(col("id_produk")))
    # Ubah jumlah ke IntegerType
    .withColumn("jumlah", col("jumlah").cast(IntegerType()))
    # Ubah harga_satuan ke IntegerType
    .withColumn("harga_satuan", col("harga_satuan").cast(IntegerType()))
    # Tambahkan kolom total_harga = jumlah * harga_satuan
    .withColumn("total_harga", (col("jumlah") * col("harga_satuan")).cast(IntegerType()))
    # Isi semua kolom null (string -> "", angka -> 0, tanggal -> default)
    .fillna({
        "id_transaksi": "",
        "id_produk": "",
        "jumlah": 0,
        "harga_satuan": 0,
        "total_harga": 0,
        "tanggal_transaksi": "1970-01-01"
    })
)
# Urutkan kolom biar tanggal_transaksi paling kanan
df_bronze = df_bronze.select(
    "id_transaksi",
    "id_produk",
    "jumlah",
    "harga_satuan",
    "total_harga",
    "tanggal_transaksi"
)
# Tampilkan schema dan isi data
#df_bronze.printSchema()
df_bronze.show()
# Simpan hasil bronze ke file Parquet
# (Parquet lebih efisien untuk data warehouse)
df_bronze.write.mode("overwrite").parquet("../bronze_output.parquet")
