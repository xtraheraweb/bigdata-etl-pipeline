import docker
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime

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

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='idx_pipeline',
    default_args=default_args,
    schedule_interval='@monthly',  # IDX reports are typically monthly
    catchup=False,
    tags=['idx', 'reports', 'emiten', 'tugas_bigdata'],
) as dag:
    
    # TASK: Extract IDX data
    extract_idx = DockerOperator(
        task_id='extract_idx',
        image='bigdata-pipeline:latest',  # Menggunakan image yang sudah di-build
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bigdata_net",  # Updated network name
        mount_tmp_dir=False,
        mounts=[
            Mount(source=host_output_path, target='/app/output', type='bind'),
            Mount(source=host_output_path.replace('output', 'airflow/logs'), target='/app/logs', type='bind')
        ],        command=["python", "idx-extract.py"],
        container_name=f"pipeline_extract_idx_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        environment={
            'IDX_DOWNLOAD_DIR': '/app/output',
            'PYTHONUNBUFFERED': '1',
            'DISPLAY': ':99'  # Virtual display for headless browser
        },
        # Increased timeout for lengthy web scraping operation
        api_version='auto',
        timeout=3600  # 1 hour timeout
    )
    
    # TASK: Load IDX data to MongoDB
    load_idx = DockerOperator(
        task_id='load_idx',
        image='bigdata-pipeline:latest',  # Using the same image since we consolidated
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bigdata_net",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=host_output_path, target='/app/output', type='bind'),
            Mount(source=host_output_path.replace('output', 'airflow/logs'), target='/app/logs', type='bind')
        ],        command=["python", "idx-load.py"],
        container_name=f"pipeline_load_idx_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        environment={
            'MONGO_URI': 'mongodb+srv://bigdatakecil:bigdata04@xtrahera.m7x7qad.mongodb.net/?retryWrites=true&w=majority&appName=xtrahera',
            'MONGO_DB': 'tugas_bigdata',
            'MONGO_COLLECTION': 'idx_raw',
            'IDX_DOWNLOAD_DIR': '/app/output',
            'PYTHONUNBUFFERED': '1'
        },    )

    # TASK: Transform IDX data with PySpark
    transform_idx = DockerOperator(
        task_id='transform_idx',
        image='bigdata-pipeline:latest',
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bigdata_net",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=host_output_path, target='/app/output', type='bind'),
            Mount(source=host_output_path.replace('output', 'airflow/logs'), target='/app/logs', type='bind')
        ],
        command=["python", "/app/idx-transform.py"],
        container_name="pipeline_transform_idx",
        environment={
            'MONGO_URI': 'mongodb+srv://bigdatakecil:bigdata04@xtrahera.m7x7qad.mongodb.net/?retryWrites=true&w=majority&appName=xtrahera',
            'MONGO_DB': 'tugas_bigdata',
            'MONGO_COLLECTION': 'idx_raw',
            'MONGO_TRANSFORM_COLLECTION': 'idx_transform',
            'SPARK_HOME': '/opt/spark',
            'PYTHONUNBUFFERED': '1'
        }
    )
    
    # Set up task dependencies
    extract_idx >> load_idx >> transform_idx
    