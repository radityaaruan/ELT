from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# --- PREJOB FUNCTION ---
def cek_koneksi_postgre():
    """
    Pre-job: Mengecek apakah koneksi ke PostgreSQL berhasil
    """
    import psycopg2
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="etl_pegadaian",
            user="postgres",
            password="postgres"
        )
        conn.close()
        print("Koneksi PostgreSQL berhasil.")
    except Exception as e:
        raise Exception(f"Gagal konek PostgreSQL: {e}")

# --- ETL TASK FUNCTIONS ---
def run_silver_etl():
    """
    Menjalankan script untuk silver layer
    """
    subprocess.run(["python3", "silver_etl.py"])

def run_bronze_etl():
    """
    Menjalankan script untuk bronze layer
    """
    subprocess.run(["python3", "bronze_etl.py"])

def run_gold_etl():
    """
    Menjalankan script untuk gold layer dan simpan ke PostgreSQL
    """
    subprocess.run(["python3", "gold_etl.py"])

# --- POSTJOB FUNCTION ---
def log_selesai():
    """
    Post-job: Catat bahwa pipeline sudah selesai
    """
    print(f"ETL Pipeline selesai dijalankan pada {datetime.now()}")

# --- DAG DEFINITION ---
with DAG(
    dag_id="etl_penjualan_dag",
    description="ETL DAG Penjualan Pegadaian (Silver -> Bronze -> Gold)",
    schedule_interval="@daily",   # bisa diganti misalnya "0 7 * * *" (jam 7 pagi)
    start_date=datetime(2025, 7, 7),
    catchup=False,
    tags=["ETL", "Pegadaian"]
) as dag:

    # Pre-job task
    cek_postgre_task = PythonOperator(
        task_id="cek_koneksi_postgre",
        python_callable=cek_koneksi_postgre
    )

    # ETL task
    silver_task = PythonOperator(
        task_id="silver_etl",
        python_callable=run_silver_etl
    )

    bronze_task = PythonOperator(
        task_id="bronze_etl",
        python_callable=run_bronze_etl
    )

    gold_task = PythonOperator(
        task_id="gold_etl",
        python_callable=run_gold_etl
    )

    # Post-job task
    selesai_task = PythonOperator(
        task_id="log_selesai",
        python_callable=log_selesai
    )

    # Urutan eksekusi: prejob -> silver -> bronze -> gold -> postjob
    cek_postgre_task >> silver_task >> bronze_task >> gold_task >> selesai_task
