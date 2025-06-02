FROM bitnami/spark:3.4.1

USER root
RUN install_packages python3 python3-pip && \
    pip3 install boto3 python-dotenv

WORKDIR /app
COPY . /app

ENV PYSPARK_PYTHON=python3
