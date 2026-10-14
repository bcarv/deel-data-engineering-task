FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.1
ARG SCALA_BINARY=2.12
ARG POSTGRES_JDBC_VERSION=42.7.4
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "pyspark==${SPARK_VERSION}"

ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV JAVA_HOME=/opt/java-home

WORKDIR /opt/spark-app

# JDBC driver + spark-sql-kafka (and runtime deps) into Spark's classpath.
RUN set -eux; \
    java_home="$(dirname "$(dirname "$(readlink -f "$(which java)")")")"; \
    ln -sfn "${java_home}" /opt/java-home; \
    jars="${SPARK_HOME}/jars"; \
    curl -fsSL -o "${jars}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    curl -fsSL -o "${jars}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar"; \
    curl -fsSL -o "${jars}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar"; \
    curl -fsSL -o "${jars}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSL -o "${jars}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
 
# Application code
COPY app/ /opt/spark-app/
 
# local[2]: driver + executors in a single JVM with 2 cores — right-sized for the
# demo stack. Production sizing strategy is documented in diagrams/architecture.
CMD ["spark-submit", "--master", "local[2]", "/opt/spark-app/streaming_job.py"]
