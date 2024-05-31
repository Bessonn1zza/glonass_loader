FROM apache/airflow:2.5.1-python3.10

USER root
RUN apt-get update \
    && apt-get -y install libpq-dev gcc

USER airflow
COPY requirements.txt ./
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt