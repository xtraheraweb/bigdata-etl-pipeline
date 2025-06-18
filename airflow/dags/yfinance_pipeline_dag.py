import docker
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

# Configuration (menggunakan database tunggal)
used_path = '/app/output/tickers_data.json'
mongo_db = 'tugas_bigdata'  # Database tunggal untuk semua data

# Mendapatkan path host yang benar dari mount container saat ini
try:
    client = docker.from_env()
    current_container = client.containers.get(os.environ['HOSTNAME'])
    for mount in current_container.attrs['Mounts']:
        if mount['Destination'] == '/opt/airflow/output':
            host_output_path = mount['Source'] 
            break
    else:
        raise Exception("Mount point /opt/airflow/output not found in container")
    
    print("Host output path:", host_output_path)
except Exception as e:
    print(f"Warning: Could not get host path: {e}")
    host_output_path = "/tmp/output"  # Fallback

# Default args yang sudah tested
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 10),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=3),
}

with DAG(
    dag_id='yfinance_pipeline',  # NAMA SEDERHANA
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    tags=['saham', 'yfinance', 'tugas_bigdata'],
    description='yFinance pipeline for 950 Indonesian stocks',
    max_active_runs=1,
) as dag:
    
    # TASK 1: EXTRACT DATA
    extract_yfinance = DockerOperator(
        task_id='extract_yfinance',
        image='bigdata-pipeline:latest',
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bigdata_net",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=host_output_path, target='/app/output', type='bind')
        ],
        # MENGGUNAKAN script yang sudah ada di image
        command="python yfinance_data_fetcher_5years.py",
        container_name="pipeline_extract",
        environment={
            'EXCEL_FILE_PATH': '/app/tickers.xlsx',
            'YFINANCE_OUTPUT_PATH': used_path,
            'PYTHONUNBUFFERED': '1',
            # Environment variables untuk rate limiting yang aman
            'MAX_STOCKS': '950',
            'DELAY_BETWEEN_REQUESTS': '5',
            'BATCH_SIZE': '5',
            'DELAY_BETWEEN_BATCHES': '30',
            'MAX_RETRIES': '3'
        },
        execution_timeout=timedelta(hours=2),
    )
    
    # TASK 2: LOAD TO MONGODB
    load_yfinance = DockerOperator(
        task_id='load_yfinance',
        image='bigdata-pipeline:latest',
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bigdata_net",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=host_output_path, target='/app/output', type='bind')
        ],
        # MENGGUNAKAN script yang sudah ada di image
        command="python yfinance-load.py",
        container_name="pipeline_load",
        environment={
            'MONGO_URI': 'mongodb+srv://bigdatakecil:bigdata04@xtrahera.m7x7qad.mongodb.net/?retryWrites=true&w=majority&appName=xtrahera',
            'MONGO_DB': mongo_db,
            'INPUT_PATH': used_path,
            'PYTHONUNBUFFERED': '1',
        },
        execution_timeout=timedelta(minutes=45),
    )
    
    # TASK 3: CLEANUP
    cleanup_temp_files = DockerOperator(
        task_id='cleanup_temp_files',
        image='bigdata-pipeline:latest',
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bigdata_net",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=host_output_path, target='/app/output', type='bind')
        ],
        command="sh -c 'rm -f /app/output/*.json /app/output/*.log || true'",
        container_name="pipeline_cleanup",
        trigger_rule='all_done',
        execution_timeout=timedelta(minutes=5),
    )
    
    # Task dependencies
    extract_yfinance >> load_yfinance >> cleanup_temp_files