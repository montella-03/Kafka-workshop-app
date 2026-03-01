FROM confluentinc/cp-kafka:7.7.2

USER root
RUN mkdir -p /opt/jmx-exporter \
    && curl -fsSL https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar \
    -o /opt/jmx-exporter/jmx_prometheus_javaagent.jar
COPY monitoring/jmx/kafka-jmx.yml /opt/jmx-exporter/kafka-jmx.yml

USER appuser
