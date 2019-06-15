DOCKER_REGISTRY=emptymalei/geoeconomics-with-data
TAG?=latest
DOCKER_IMAGE?=${DOCKER_REGISTRY}:${TAG}

build:
	rm -rf dist
	pip install -r requirements.txt --force --ignore-installed
	python setup.py sdist
	cp dist/app-* deploy/app.tar.gz
	cp requirements.txt deploy/requirements.txt
	cd deploy/ && docker build -t ${DOCKER_IMAGE} .
	python setup.py clean --all
	rm -rf dist
	rm -rf deploy/app.*

publish:
	docker tag ${DOCKER_IMAGE} ${DOCKER_REGISTRY}
	docker push ${DOCKER_REGISTRY}
