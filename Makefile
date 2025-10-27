.PHONY: build_for_tests
build_for_tests:
	./gradlew clean systemTestLibs

.PHONY: original_docker_image
original_docker_image:
	tests/docker/ducker-ak up
	tests/docker/ducker-ak down

.PHONY: docker_image
docker_image:
	docker build . -t us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/kafka-systest-experiment:latest

.PHONY: push_docker_image
push_docker_image:
	docker push us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/kafka-systest-experiment:latest

CONFIG_IMAGE := 'us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/kafka-systest-experiment-config:latest'
.PHONY: build_and_push_config_docker_image
build_and_push_config_docker_image:
	docker build -f Dockerfile-config . -t $(CONFIG_IMAGE)
	docker push $(CONFIG_IMAGE)
