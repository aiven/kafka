FROM apache/kafka:4.3.0-rc0

USER root

COPY core/build/distributions/kafka_2.13-4.2.0-SNAPSHOT.tgz /tmp/kafka.tgz

RUN set -eux; \
    find /opt/kafka -mindepth 1 -delete; \
    tar xzf /tmp/kafka.tgz -C /opt/kafka --strip-components 1; \
    cp /opt/kafka/config/log4j2.yaml /etc/kafka/docker/log4j2.yaml; \
    cp /opt/kafka/config/tools-log4j2.yaml /etc/kafka/docker/tools-log4j2.yaml; \
    chown -R appuser:appuser /opt/kafka; \
    rm /tmp/kafka.tgz

USER appuser
