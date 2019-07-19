format:
	isort -y
	black .

test:
	pytest tests.py --cov aiochclient -x -v

tests: test

upload: build
	if [ -d dist ]; then rm -Rf dist; fi
	python setup.py sdist
	twine upload dist/*

check_format:
	isort --check --diff
	black . --check --diff --target-version py36

build:
	 python setup.py build_ext --inplace
