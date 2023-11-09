format:
	isort .
	black .

test: install-dev-requirements docker-clickhouse
	pytest tests.py --cov aiochclient -x -vv

tests: test

upload: build_cython
	if [ -d dist ]; then rm -Rf dist; fi
	python setup.py sdist
	twine upload dist/*

check_format:
	isort --check --diff
	black . --check --diff --target-version py36

build_cython:
	python setup.py build_ext --inplace

html_types:
	cython -a aiochclient/_types.pyx

docker-clickhouse:
	docker pull yandex/clickhouse-server
	docker start cs || docker run -p 8123:8123 -d --name cs yandex/clickhouse-server

install-dev-requirements:
	pip install -r dev-requirements/dev-requirements-cython-ciso.txt --upgrade
