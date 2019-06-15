DOCKER_REGISTRY=emptymalei/geoeconomics-with-data
TAG?=latest
DOCKER_IMAGE?=${DOCKER_REGISTRY}:${TAG}

build:
	rm -rf dist
	pip install -r requirements.txt
	python setup.py sdist
	cp dist/app-* deploy/app.tar.gz
	cp requirements.txt deploy/requirements.txt
	cd deploy/ && docker build -t ${DOCKER_IMAGE} .
	python setup.py clean --all
	rm -rf dist
	rm -rf hl.app.*
	rm -rf deploy/app.*

publish:
	docker tag ${DOCKER_IMAGE} ${DOCKER_PATH}
	docker push ${DOCKER_PATH}
