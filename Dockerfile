FROM bitnami/spark:3.4.1
USER root
WORKDIR /app
COPY requirements.txt .
RUN install_packages python3 python3-pip && \
    pip3 install --no-cache-dir -r requirements.txt

COPY . /app

ENV PYSPARK_PYTHON=python3