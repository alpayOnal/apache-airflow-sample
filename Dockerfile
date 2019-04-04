FROM python:3.6-slim

WORKDIR /test_airflow

RUN apt-get update
RUN apt-get install -y supervisor gcc procps
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

RUN mkdir  /root/.aws/
COPY aws_credential  /root/.aws/credentials
COPY aws_config  /root/.aws/config

COPY requirements.txt /tmp/

ENV AIRFLOW_HOME=/test_airflow/airflow
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes

RUN export SLUGIFY_USES_TEXT_UNIDECODE=yes

RUN pip install --no-cache-dir -r /tmp/requirements.txt;
RUN airflow initdb
EXPOSE 8080

CMD ["/usr/bin/supervisord"]