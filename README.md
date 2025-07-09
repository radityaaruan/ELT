# 🏦 Pegadaian ETL Data Dummy Pipeline

![Python](https://img.shields.io/badge/python-v3.9+-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.0+-orange.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.0+-yellow.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-blue.svg)

An end-to-end ETL (Extract, Transform, Load) data pipeline built for Pegadaian (Indonesian pawn shop chain) sales data processing using modern data engineering tools and medallion architecture.

## 📊 Project Overview

This project implements a robust, scalable ETL pipeline that processes sales transaction data through multiple layers of transformation, following the **Medallion Architecture** (Bronze-Silver-Gold) pattern for optimal data quality and governance.

### 🎯 Business Problem
- Manual data processing was time-consuming and error-prone
- Need for automated daily sales reporting
- Requirement for data quality validation and monitoring
- Scalable solution for growing data volumes

### 💡 Solution
Automated ETL pipeline with workflow orchestration, data quality checks, and real-time monitoring capabilities.

## 🏗️ Architecture

```
📁 Raw CSV Data
    ↓
🥈 Silver Layer (Data Ingestion)
    ↓ 
🥉 Bronze Layer (Data Cleaning & Transformation)
    ↓
🥇 Gold Layer (Business-Ready Data)
    ↓
🗄️ PostgreSQL Data Warehouse
```

### Data Flow:
1. **Silver Layer**: Raw CSV files → Clean Parquet format
2. **Bronze Layer**: Data standardization, null handling, calculated fields
3. **Gold Layer**: Final enrichment with timestamps → PostgreSQL

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Orchestration** | Apache Airflow | Workflow management & scheduling |
| **Processing** | PySpark | Distributed data processing |
| **Database** | PostgreSQL | Data warehouse |
| **Language** | Python 3.9+ | ETL scripting |
| **Storage** | Parquet | Intermediate data storage |

## 📂 Project Structure

```
pegadaian/
├── README.md                     # Project documentation
├── requirements.txt              # Python dependencies
├── .gitignore                   # Git ignore rules
├── 
├── airflow/                     # Airflow configuration
│   ├── airflow.cfg             # Airflow settings
│   ├── dags/                   # DAG definitions
│   │   └── etl_penjualan_dag.py # Main ETL DAG
│   └── logs/                   # Airflow logs
│
├── data/                       # Data files
│   └── penjualan_2025-07-03.csv # Sample sales data
│
├── etl_scripts/                # ETL processing scripts
│   ├── silver_etl.py          # Silver layer processing
│   ├── bronze_etl.py          # Bronze layer processing
│   └── gold_etl.py            # Gold layer processing
│
├── config/                     # Configuration files
│   └── postgresql-42.2.20.jar # PostgreSQL JDBC driver
│
└── output/                     # Generated output files
    ├── silver_output.parquet/
    └── bronze_output.parquet/
```

## ⚡ Features

- ✅ **Automated Daily Processing**: Scheduled runs via Airflow
- ✅ **Data Quality Validation**: Built-in checks and error handling
- ✅ **Fault Tolerance**: Retry mechanisms and failure notifications
- ✅ **Monitoring & Logging**: Real-time pipeline monitoring
- ✅ **Scalable Architecture**: Medallion pattern for data governance
- ✅ **Database Integration**: Direct PostgreSQL integration
- ✅ **Modular Design**: Separate ETL scripts for maintainability

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Apache Airflow 2.0+
- PostgreSQL 13+
- Java 8+ (for Spark)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/pegadaian-etl-pipeline.git
cd pegadaian-etl-pipeline
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Set up PostgreSQL database**
```sql
CREATE DATABASE etl_pegadaian;
CREATE SCHEMA gold;
```

4. **Configure Airflow**
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
```

5. **Start the pipeline**
```bash
# Start Airflow scheduler
airflow scheduler &

# Start Airflow webserver
airflow webserver --port 8080 &
```

6. **Access Airflow UI**
Open http://localhost:8080 in your browser

## 🔧 Configuration

### Database Settings
Update the connection parameters in the DAG file:
```python
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="etl_pegadaian",
    user="your_username",
    password="your_password"
)
```

### File Paths
Ensure the JDBC driver path is correct in `gold_etl.py`:
```python
.config("spark.jars", "/path/to/postgresql-42.2.20.jar")
```

## 📈 Pipeline Stages

### 1. Silver Layer (`silver_etl.py`)
- **Input**: Raw CSV files
- **Process**: Basic data ingestion and format conversion
- **Output**: Clean Parquet files
- **Transformations**: 
  - CSV to Parquet conversion
  - Schema validation
  - Basic data type casting

### 2. Bronze Layer (`bronze_etl.py`)
- **Input**: Silver Parquet files
- **Process**: Data cleaning and standardization
- **Output**: Processed Parquet files
- **Transformations**:
  - Null value handling (replace with 0)
  - Product code standardization (uppercase)
  - Total price calculation
  - Data validation

### 3. Gold Layer (`gold_etl.py`)
- **Input**: Bronze Parquet files
- **Process**: Final enrichment and loading
- **Output**: PostgreSQL `gold.penjualan` table
- **Transformations**:
  - Timestamp addition
  - Column reordering
  - Database insertion

## 📊 Sample Data

The pipeline processes sales transaction data with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `id_transaksi` | String | Transaction ID |
| `id_produk` | String | Product ID |
| `jumlah` | Integer | Quantity |
| `harga_satuan` | Decimal | Unit price |
| `total_harga` | Decimal | Total amount |
| `tanggal_transaksi` | Date | Transaction date |
| `inserted_at` | Timestamp | ETL processing time |

## 🔍 Monitoring & Troubleshooting

### Airflow Web UI
- **DAG Overview**: http://localhost:8080
- **Task Logs**: Available in Airflow UI under each task
- **Pipeline Status**: Real-time monitoring of task execution

### Common Issues

1. **Script not found error**
   - Check file paths in the DAG
   - Verify AIRFLOW_HOME is set correctly

2. **Database connection issues**
   - Verify PostgreSQL is running
   - Check connection parameters
   - Ensure database and schema exist

3. **Spark/Java issues**
   - Verify Java installation
   - Check JDBC driver path
   - Ensure sufficient memory allocation

## 📝 Development

### Adding New Transformations
1. Create new ETL script in the project root
2. Add task to the DAG in `etl_penjualan_dag.py`
3. Update task dependencies as needed

### Testing
```bash
# Test individual ETL scripts
python3 silver_etl.py
python3 bronze_etl.py
python3 gold_etl.py

# Test full DAG
airflow dags test etl_penjualan_dag 2025-07-08
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**Raditya Aruan**
- LinkedIn: radityaaruan
- GitHub: @radityaaruan
- Email: radityaruan@gmail.com

## 🙏 Acknowledgments

- Apache Airflow community for excellent documentation
- PySpark team for powerful data processing capabilities
- PostgreSQL community for robust database solution

---

⭐ If you found this project helpful, please give it a star!
