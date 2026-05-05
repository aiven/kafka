.PHONY: docker_image
docker_image:
	docker build -t kafka-mirroring:local .
