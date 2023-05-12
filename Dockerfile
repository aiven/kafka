##
# Copyright 2023 Aiven Oy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##
FROM ivanyuaiven/kafka-base:0.1.0

RUN dnf -y update && dnf install -y java-11-openjdk \
    && dnf clean all

ARG _SCALA_VERSION
ARG _KAFKA_VERSION
ENV _KAFKA_FULL_VERSION "kafka_${_SCALA_VERSION}-${_KAFKA_VERSION}"

ENV KAFKA_LOG_DIR /var/lib/kafka/data

COPY core/build/distributions/${_KAFKA_FULL_VERSION}.tgz /kafka
RUN tar -xf ${_KAFKA_FULL_VERSION}.tgz --strip-components=1 \
   && rm ${_KAFKA_FULL_VERSION}.tgz \
   && rm libs/connect-*-${_KAFKA_VERSION}.jar \
   && rm libs/kafka-streams-*-${_KAFKA_VERSION}.jar \
   && mkdir -p ${KAFKA_LOG_DIR}

COPY clients/build/libs/kafka-clients-${_KAFKA_VERSION}-test.jar /kafka/libs
COPY storage/build/libs/kafka-storage-${_KAFKA_VERSION}-test.jar /kafka/libs
COPY storage/api/build/libs/kafka-storage-api-${_KAFKA_VERSION}-test.jar /kafka/libs

COPY run.sh /kafka

ENV INCLUDE_TEST_JARS=true

CMD /kafka/run.sh
