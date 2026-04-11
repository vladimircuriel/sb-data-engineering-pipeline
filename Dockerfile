FROM apache/airflow:3.2.0

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
