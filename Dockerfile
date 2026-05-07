FROM apache/airflow:2.11.2

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip cache purge

USER airflow