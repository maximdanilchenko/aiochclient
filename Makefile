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
	docker pull clickhouse/clickhouse-server
	docker start cs 2>/dev/null || docker run -p 8123:8123 -d --name cs -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server
	@echo "Waiting for ClickHouse to become ready..."
	@for i in $$(seq 1 60); do \
		if curl -fs http://localhost:8123/ping | grep -q Ok; then echo "ClickHouse is ready"; exit 0; fi; \
		sleep 1; \
	done; \
	echo "ClickHouse did not become ready in time" && exit 1

install-dev-requirements:
	pip install twine
	pip install -r dev-requirements/dev-requirements-cython-ciso.txt --upgrade
