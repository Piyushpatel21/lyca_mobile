MAKE_DIR = $(PWD)

RRBS_DIR := phase2/pyspark_etl/RRBS
RRBS_OTL_DIR := phase2/pyspark_etl/RRBS_OTL
MNO_DIR := phase2/pyspark_etl/MNO
MNO_OTL_DIR := phase2/pyspark_etl/MNO_OTL
PROJECTS := $(RRBS_DIR) $(MNO_DIR)

.PHONY: build_all build_rrbs build_rrbs_otl build_mno build_mno_otl clean

build_all: clean build_rrbs build_mno build_rrbs_otl build_mno_otl

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

build_rrbs_otl:
	$(MAKE) --directory=$(RRBS_OTL_DIR) build
	mkdir -p dist/RRBS_OTL/code dist/RRBS_OTL/configs/ dist/RRBS_OTL/schemas dist/RRBS_OTL/job_configs
	cp $(RRBS_OTL_DIR)/dist/* dist/RRBS_OTL/code/
	cp $(RRBS_OTL_DIR)/code/pythonlib/main/src/main.py dist/RRBS_OTL/code/
	cp $(RRBS_OTL_DIR)/code/config/*.json dist/RRBS_OTL/schemas
	cp $(RRBS_OTL_DIR)/config/*.json dist/RRBS_OTL/configs/
	cp $(RRBS_OTL_DIR)/job_configs/*.json dist/RRBS_OTL/job_configs/

build_mno:
	$(MAKE) --directory=$(MNO_DIR) build
	mkdir -p dist/MNO/code dist/MNO/configs/ dist/MNO/schemas dist/MNO/job_configs
	cp $(MNO_DIR)/dist/* dist/MNO/code/
	cp $(MNO_DIR)/code/pythonlib/main/src/main.py dist/MNO/code/
	cp $(MNO_DIR)/code/config/*.json dist/MNO/schemas
	cp $(MNO_DIR)/config/*.json dist/MNO/configs/
	cp $(MNO_DIR)/job_configs/*.json dist/MNO/job_configs/

build_mno_otl:
	$(MAKE) --directory=$(MNO_OTL_DIR) build
	mkdir -p dist/MNO_OTL/code dist/MNO_OTL/configs/ dist/MNO_OTL/schemas dist/MNO_OTL/job_configs
	cp $(MNO_OTL_DIR)/dist/* dist/MNO_OTL/code/
	cp $(MNO_OTL_DIR)/code/pythonlib/main/src/main.py dist/MNO_OTL/code/
	cp $(MNO_OTL_DIR)/code/config/*.json dist/MNO_OTL/schemas
	cp $(MNO_OTL_DIR)/config/*.json dist/MNO_OTL/configs/
	cp $(MNO_OTL_DIR)/job_configs/*.json dist/MNO_OTL/job_configs/

