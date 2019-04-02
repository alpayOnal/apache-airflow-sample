FROM python:3.6-slim

RUN apt-get update

RUN apt-get install -y supervisor gcc procps
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
ENV AIRFLOW_HOME=/app/airflow
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes


RUN export SLUGIFY_USES_TEXT_UNIDECODE=yes
RUN pip install apache-airflow
RUN airflow initdb
EXPOSE 8080

#CMD ["/usr/bin/supervisord"]