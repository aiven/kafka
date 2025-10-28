.PHONY: build_for_tests
build_for_tests:
	./gradlew clean systemTestLibs

.PHONY: original_docker_image
original_docker_image:
	tests/docker/ducker-ak up
	tests/docker/ducker-ak down

.PHONY: docker_images
docker_images:
	docker build . -f antithesis/Dockerfile-driver \
		-t us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/kafka-systest-experiment-driver:latest
	docker build . -f antithesis/Dockerfile-vm \
		-t us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/kafka-systest-experiment-vm:latest

.PHONY: push_docker_images
push_docker_images:
	docker push us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/kafka-systest-experiment-driver:latest
	docker push us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/kafka-systest-experiment-vm:latest

CONFIG_IMAGE := 'us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/kafka-systest-experiment-config:latest'
.PHONY: build_and_push_config_docker_image
build_and_push_config_docker_image:
	docker build . -f antithesis/Dockerfile-config -t $(CONFIG_IMAGE)
	docker push $(CONFIG_IMAGE)
