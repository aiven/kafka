FROM ducker-ak-openjdk-17-bullseye

USER root
RUN mkdir -p /opt/kafka-dev
RUN chown ducker /opt/kafka-dev
# Autoinstrumentation of JARs.
# https://antithesis.com/docs/using_antithesis/sdk/java/instrumentation/
RUN mkdir -p /opt/antithesis/catalog
RUN ln -s /opt/kafka/libs/ /opt/antithesis/catalog
USER ducker

RUN pip install antithesis

ADD cluster.json /opt/kafka-dev
ADD tests /opt/kafka-dev/tests
ADD bin /opt/kafka-dev/bin
ADD core/build/libs /opt/kafka-dev/libs
ADD core/build/dependant-libs-2.13.16 /opt/kafka-dev/libs
ADD core/build/dependant-testlibs /opt/kafka-dev/libs
ADD tools/build/libs /opt/kafka-dev/libs
ADD docker-entrypoint.sh /opt/kafka-dev
