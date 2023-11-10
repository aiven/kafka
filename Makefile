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
SCALA_VERSION := $(shell grep -o -E '^scalaVersion=[0-9]+\.[0-9]+\.[0-9]+' gradle.properties | cut -c14-)
SCALA_MAJOR_VERSION := $(shell grep -o -E '^scalaVersion=[0-9]+\.[0-9]+' gradle.properties | cut -c14-)
KAFKA_VERSION := $(shell grep -o -E '^version=[0-9]+\.[0-9]+\.[0-9]+(-SNAPSHOT)?' gradle.properties | cut -c9-)
IMAGE_NAME=aivenoy/kafka

BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
IMAGE_TAG := $(subst /,_,$(BRANCH))

.PHONY: all build clean

all: clean build

clean:
	./gradlew clean

build: core/build/distributions/kafka_$(SCALA_VERSION)-$(KAFKA_VERSION).tgz

core/build/distributions/kafka_$(SCALA_VERSION)-$(KAFKA_VERSION).tgz:
	echo "Version: $(KAFKA_VERSION)-$(SCALA_VERSION)"
	./gradlew -PscalaVersion=$(SCALA_VERSION) testJar releaseTarGz

.PHONY: docker_image
docker_image: build
	docker build . \
		--build-arg _SCALA_VERSION=$(SCALA_MAJOR_VERSION) \
		--build-arg _KAFKA_VERSION=$(KAFKA_VERSION) \
		-t $(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: docker_push
docker_push:
	docker push $(IMAGE_NAME):$(IMAGE_TAG)

# TODO publish docker images


.PHONY: failure-demo-prepare
failure-demo-prepare:
	git submodule init
	git submodule update
	cd tiered-storage-for-apache-kafka && make build
	cd tiered-storage-for-apache-kafka/build/distributions && tar -xf tiered-storage-for-apache-kafka-0.0.1-SNAPSHOT.tgz
	cd tiered-storage-for-apache-kafka/storage/gcs/build/distributions/ && tar -xf gcs-0.0.1-SNAPSHOT.tgz
	./gradlew releaseTarGz -x test
	cd core/build/distributions && tar -xf kafka_2.13-3.6.0.tgz

.PHONY: failure-demo-clean
failure-demo-clean:
	docker compose down
	rm -rf _failure_demo_kafka_data && git checkout -- _failure_demo_kafka_data
	rm -rf _failure_demo_local_cache && git checkout -- _failure_demo_local_cache

.PHONY: failure-demo-run-zk
failure-demo-run-zk:
	docker compose up -d

.PHONY: failure-demo-create-topic
failure-demo-create-topic:
	core/build/distributions/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --topic topic1 \
		--config remote.storage.enable=true \
		--config segment.bytes=512000 \
		--config local.retention.bytes=1 \
		--config retention.bytes=10000000000000

.PHONY: failure-demo-fill-topic
failure-demo-fill-topic:
	core/build/distributions/kafka_2.13-3.6.0/bin/kafka-producer-perf-test.sh \
		--topic topic1 --num-records=5000 --throughput -1 --record-size 1000 \
		--producer-props acks=1 batch.size=16384 bootstrap.servers=localhost:9092

.PHONY: failure-demo-consume
failure-demo-consume:
	kcat -b localhost:9092 -t topic1 -o 0 -f "%o\n" -e -X fetch.wait.max.ms=3000

.PHONY: failure-demo-delete-topic
failure-demo-delete-topic:
	core/build/distributions/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--delete --topic topic1
