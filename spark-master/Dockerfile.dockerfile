FROM bde2020/spark-master:3.3.0-hadoop3.3

USER root
RUN apt-get update && apt-get install -y coreutils procps bash \
    && rm -rf /var/lib/apt/lists/*
USER root