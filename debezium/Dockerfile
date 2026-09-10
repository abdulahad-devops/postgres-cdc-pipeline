ARG DEBEZIUM_VERSION=3.6.2.Final
FROM quay.io/debezium/connect:${DEBEZIUM_VERSION}

ARG JMX_EXPORTER_VERSION=1.0.0

RUN mkdir -p /kafka/etc && \
    curl --fail --silent --show-error --location \
      --output /kafka/etc/jmx_prometheus_javaagent.jar \
      "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar"

COPY jmx-exporter.yml /kafka/etc/jmx-exporter.yml
