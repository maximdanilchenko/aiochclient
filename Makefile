format:
	isort -y
	black .

test:
	pytest tests.py --cov aiochclient -x -v

upload:
	if [ -d dist ]; then rm -Rf dist; fi
	python setup.py sdist
	twine upload dist/*
