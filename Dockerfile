FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    xvfb \
    chromium \
    chromium-driver \
    gnupg \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set Java Home
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Apache Spark
ENV SPARK_VERSION=3.2.0
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Download and setup Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.2.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3.2 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.2.tgz
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.2.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop3.2 /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.2.tgz

# Add Spark to PATH
ENV PATH=$PATH:$SPARK_HOME/bin

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DISPLAY=:99
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Create working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY airflow/dags/requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt -v --use-feature=fast-deps

# Copy all Python scripts and data files
COPY airflow/dags/ /app/
COPY airflow/dags/tickers.xlsx /app/

# Create necessary directories
RUN mkdir -p /app/output /app/logs

# Set executable permissions for scripts
RUN chmod +x /app/*.py

# Set environment variables for MongoDB
ENV MONGO_URI="mongodb+srv://bigdatakecil:bigdata04@xtrahera.m7x7qad.mongodb.net/?retryWrites=true&w=majority&appName=xtrahera"
ENV MONGO_DB="bigdata_saham"
ENV MONGO_COLLECTION="idx2024"
ENV IDX_DOWNLOAD_DIR="/app/output"

# Health check to ensure the application is ready
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=5 \
    CMD curl --fail http://localhost:8080/health || exit 1

# Default command (can be overridden by docker-compose)
CMD ["python", "--version"]