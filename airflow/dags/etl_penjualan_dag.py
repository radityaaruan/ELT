from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import os

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
        print("✅ Koneksi PostgreSQL berhasil.")
    except Exception as e:
        raise Exception(f"❌ Gagal konek PostgreSQL: {e}")

# --- ETL TASK FUNCTIONS ---
def run_script(script_name):
    """
    Menjalankan script Python eksternal
    """
    # Script berada di /Users/radityaaruan/pegadaian/etl_scripts/
    script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "etl_scripts", script_name)
    print(f"🔍 Mencari script di: {script_path}")
    
    if not os.path.exists(script_path):
        raise Exception(f"❌ Script tidak ditemukan: {script_path}")
    
    result = subprocess.run(["python3", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"❌ Gagal menjalankan {script_name}:\n{result.stderr}")
    else:
        print(f"✅ Berhasil menjalankan {script_name}:\n{result.stdout}")

# --- POSTJOB FUNCTION ---
def log_selesai():
    """
    Post-job: Catat bahwa pipeline sudah selesai
    """
    print(f"✅ ETL Pipeline selesai dijalankan pada {datetime.now()}")

# --- DAG DEFINITION ---
default_args = {
    'owner': 'raditya',
    'retries': 1,
}

default_dag = DAG(
    dag_id="etl_penjualan_dag",
    description="ETL DAG Penjualan Pegadaian (Silver -> Bronze -> Gold)",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 7, 7),
    catchup=False,
    tags=["ETL", "Pegadaian"]
)

with default_dag:

    # Pre-job
    cek_postgre_task = PythonOperator(
        task_id="cek_koneksi_postgre",
        python_callable=cek_koneksi_postgre
    )

    # ETL tasks
    silver_task = PythonOperator(
        task_id="silver_etl",
        python_callable=lambda: run_script("silver_etl.py")
    )

    bronze_task = PythonOperator(
        task_id="bronze_etl",
        python_callable=lambda: run_script("bronze_etl.py")
    )

    gold_task = PythonOperator(
        task_id="gold_etl",
        python_callable=lambda: run_script("gold_etl.py")
    )

    # Post-job
    selesai_task = PythonOperator(
        task_id="log_selesai",
        python_callable=log_selesai
    )

    # Urutan eksekusi
    cek_postgre_task >> silver_task >> bronze_task >> gold_task >> selesai_task

# INI PENTING BIAR DAG TEREGISTER
dag = default_dag
