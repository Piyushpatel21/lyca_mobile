# Get the version of amorphic-utils from setup.py
VERSION=$(shell grep "version=[0-9.]*" setup.py | cut -d\" -f2)
PROJECT_NAME=$(shell grep "name=*" setup.py | cut -d\" -f2)

init:
	pip install tox==3.7.0 tox-pipenv==1.9.0

tox: init
	tox

clean:
	rm -f -r build/
	rm -f -r dist/
	rm -f -r *.egg-info

build: clean build_pyspark_zip bdist_egg

bdist_egg:
	python setup.py bdist_egg

bdist_egg_compiled:
	python setup.py bdist_egg --exclude-source-files

build_szip:
	tox -e sdist

build_pyspark_zip:
	rm -rf dist/${PROJECT_NAME}-pyspark-${VERSION}.zip
	mkdir -p libs dist
	(cd code/pythonlib/main/src; zip -r ../../../../dist/${PROJECT_NAME}-pyspark-${VERSION}.zip *)

test: init
	python -m pytest --verbose --color=yes tests/

build_docs:
	tox -e docs
