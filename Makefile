MAKE_DIR = $(PWD)

RRBS_DIR := phase2/pyspark_etl/RRBS
MNO_DIR := phase2/pyspark_etl/MNO
PROJECTS := $(RRBS_DIR) $(MNO_DIR)

.PHONY: build_all build_rrbs build_mno clean

build_all: clean build_rrbs build_mno

clean:
	rm -rf dist/

build_rrbs:
	$(MAKE) --directory=$(RRBS_DIR) build
	mkdir -p dist/RRBS/code dist/RRBS/configs/ dist/RRBS/schemas dist/RRBS/job_configs
	cp  $(RRBS_DIR)/dist/* dist/RRBS/code/
	cp $(RRBS_DIR)/code/pythonlib/main/src/main.py dist/RRBS/code/
	cp $(RRBS_DIR)/code/config/*.json dist/RRBS/schemas
	cp $(RRBS_DIR)/config/*.json dist/RRBS/configs/
	cp $(RRBS_DIR)/job_configs/*.json dist/RRBS/job_configs/

build_mno:
	$(MAKE) --directory=$(MNO_DIR) build
	mkdir -p dist/MNO/code dist/MNO/configs/ dist/MNO/schemas dist/MNO/job_configs
	cp $(MNO_DIR)/dist/* dist/MNO/code/
	cp $(MNO_DIR)/code/pythonlib/main/src/main.py dist/MNO/code/
	cp $(RRBS_DIR)/code/config/*.json dist/MNO/schemas
	cp $(MNO_DIR)/config/*.json dist/MNO/configs/
	cp $(MNO_DIR)/job_configs/*.json dist/MNO/job_configs/


