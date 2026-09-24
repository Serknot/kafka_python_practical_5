# Образ Debezium Kafka Connect уже содержит все официальные Debezium
# коннекторы (в т.ч. io.debezium.connector.postgresql.PostgresConnector),
# поэтому отдельно устанавливать плагин не нужно — расширяем образ только
# JMX-экспортёром для метрик Prometheus (Задание 2).
FROM debezium/connect:2.7

USER root

ARG JMX_EXPORTER_VERSION=0.20.0

# jmx_prometheus_javaagent публикуется как GitHub Release проекта
# prometheus/jmx_exporter.
RUN curl -sSL -o /kafka/etc/jmx_prometheus_javaagent.jar \
    "https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar"

COPY jmx-exporter-config.yml /kafka/etc/jmx-exporter-config.yml

# Официальный образ debezium/connect запускает процесс от имени
# непривилегированного пользователя "kafka" — возвращаем его перед стартом.
RUN chown kafka:kafka /kafka/etc/jmx_prometheus_javaagent.jar /kafka/etc/jmx-exporter-config.yml
USER kafka
