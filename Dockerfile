FROM apache/airflow:2.6.3

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow.db
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true

# MongoDB connection
ENV MONGO_URI="mongodb+srv://bigdatakecil:bigdata04@xtrahera.m7x7qad.mongodb.net/?retryWrites=true&w=majority&appName=xtrahera"

# Switch to root for installations
USER root

# Install system dependencies (simplified)
RUN apt-get update && apt-get install -y \
    wget curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy and install Python dependencies
COPY airflow/dags/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy DAGs and files
COPY airflow/dags/ /opt/airflow/dags/
COPY airflow/dags/tickers.xlsx /opt/airflow/tickers.xlsx

# Create directories
RUN mkdir -p /opt/airflow/logs /opt/airflow/output

# Expose port
EXPOSE 8080

# Start Airflow
CMD bash -c " \
    airflow db init && \
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin && \
    airflow webserver --port 8080 & \
    airflow scheduler & \
    wait"
