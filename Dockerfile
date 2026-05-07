FROM apache/airflow:3.2.0

USER root

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip cache purge

USER airflow