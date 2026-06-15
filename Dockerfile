FROM eclipse-temurin:17-jdk-jammy

ARG SPARK_VERSION=3.5.6
ARG HADOOP_VERSION=3
ARG SBT_VERSION=1.10.7

ENV SPARK_HOME=/opt/spark
ENV SBT_HOME=/opt/sbt
ENV PATH="${SPARK_HOME}/bin:${SBT_HOME}/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      curl \
      ca-certificates \
      bash \
      tar \
      gzip \
      procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
      -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}" \
    && rm /tmp/spark.tgz

RUN curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
      -o /tmp/sbt.tgz \
    && tar -xzf /tmp/sbt.tgz -C /opt \
    && rm /tmp/sbt.tgz

WORKDIR /app

CMD ["bash"]