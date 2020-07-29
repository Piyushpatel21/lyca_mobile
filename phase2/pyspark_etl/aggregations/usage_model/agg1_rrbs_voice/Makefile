# Get the version of amorphic-utils from setup.py
VERSION=$(shell grep "version=[0-9.]*" setup.py | cut -d\" -f2)
PROJECT_NAME=$(shell grep "name=*" setup.py | cut -d\" -f2)

init:
	pip install pytest, pyspark

clean:
	rm -f -r build/
	rm -f -r dist/
	rm -f -r *.egg-info

build: clean build_pyspark_zip

build_pyspark_zip: clean
	rm -rf dist/${PROJECT_NAME}-pyspark-${VERSION}.zip
	mkdir -p dist
	zip -r dist/${PROJECT_NAME}-pyspark-${VERSION}.zip code
	(cd ../../ ;zip -r usage_model/agg1_rrbs_voice/dist/${PROJECT_NAME}-pyspark-${VERSION}.zip parent_utils)

test: init
	python -m pytest --verbose --color=yes tests/
