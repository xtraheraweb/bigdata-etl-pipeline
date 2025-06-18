import docker
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime

# Get the correct host path from the current container's mounts
try:
    client = docker.from_env()
    current_container = client.containers.get(os.environ['HOSTNAME'])
    host_output_path = None
    for mount in current_container.attrs['Mounts']:
        if mount['Destination'] == '/opt/airflow/output':
            host_output_path = mount['Source']
            break
    if not host_output_path:
        raise Exception("Mount point /opt/airflow/output not found in Airflow container")

    print(f"Detected host output path: {host_output_path}")
except Exception as e:
    print(f"WARNING: Could not determine host output path automatically: {e}")
    host_output_path = os.path.join(os.getcwd(), 'output')
    print(f"Falling back to host output path: {host_output_path}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 10),
    'retries': 1,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='iqplus_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['saham', 'iqplus', 'tugas_bigdata'],
) as dag:

    # TASK: EXTRACT IQPlus Data
    extract_iqplus = DockerOperator(
        task_id='extract_iqplus_data',
        image='bigdata-pipeline:latest',  # Menggunakan image yang sudah di-build
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bigdata_net",  # Updated network name
        mount_tmp_dir=False,
        mounts=[
            Mount(source=host_output_path, target='/app/output', type='bind')
        ],
        command="python iqplus_extract.py",
        container_name="pipeline_iqplus_extractor",
        environment={
            'PYTHONUNBUFFERED': '1'
        },
    )
    
    # TASK: TRANSFORM IQPlus Data (Summarization)
    transform_iqplus = DockerOperator(
        task_id='transform_iqplus_data',
        image='bigdata-pipeline:latest',  # Menggunakan image yang sama
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bigdata_net",  # Updated network name
        mount_tmp_dir=False,
        mounts=[
            Mount(source=host_output_path, target='/app/output', type='bind')
        ],
        command="python iqplus-transform.py",
        container_name="pipeline_iqplus_transformer",
        environment={
            'PYTHONUNBUFFERED': '1',
            'IQPLUS_DATA_DIR': '/app/output'
        },
    )
    
    # TASK: LOAD IQPlus Data to MongoDB Atlas
    load_iqplus = DockerOperator(
        task_id='load_iqplus_data',
        image='bigdata-pipeline:latest',  # Menggunakan image yang sama
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bigdata_net",  # Updated network name
        mount_tmp_dir=False,
        mounts=[
            Mount(source=host_output_path, target='/app/output', type='bind')
        ],
        command="python iqplus-load.py",
        container_name="pipeline_iqplus_loader",
        environment={
            # MongoDB Atlas URI dengan database tunggal
            'MONGO_URI': 'mongodb+srv://bigdatakecil:bigdata04@xtrahera.m7x7qad.mongodb.net/?retryWrites=true&w=majority&appName=xtrahera',
            'MONGO_DB': 'tugas_bigdata',  # Database tunggal
            'INPUT_DIR': '/app/output',
            'PYTHONUNBUFFERED': '1'
        },
    )

    # Pipeline dependencies
    extract_iqplus >> transform_iqplus >> load_iqplus