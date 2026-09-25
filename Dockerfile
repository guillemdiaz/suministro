ARG AIRFLOW_VERSION=3.3.2

FROM apache/airflow:${AIRFLOW_VERSION}

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt