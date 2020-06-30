MAKE_DIR = $(PWD)

RRBS_DIR := phase2/pyspark_etl/RRBS
RRBS_OTL_DIR := phase2/pyspark_etl/RRBS_OTL
MNO_DIR := phase2/pyspark_etl/MNO
MNO_OTL_DIR := phase2/pyspark_etl/MNO_OTL
ACINFOLM_DIR := phase2/pyspark_etl/ACINFOLM
ACINFOLM_OTL_DIR := phase2/pyspark_etl/ACINFOLM_OTL
ACINFOTGL_DIR := phase2/pyspark_etl/ACINFOTGL
ACINFOTGL_OTL_DIR := phase2/pyspark_etl/ACINFOTGL_OTL

PROJECTS := $(RRBS_DIR) $(MNO_DIR)
ENV := dev

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

build_acinfolm:
	$(MAKE) --directory=$(ACINFOLM_DIR) build
	mkdir -p dist/ACINFOLM/code dist/ACINFOLM/configs/ dist/ACINFOLM/schemas dist/ACINFOLM/job_configs
	cp $(ACINFOLM_DIR)/dist/* dist/ACINFOLM/code/
	cp $(ACINFOLM_DIR)/code/pythonlib/main/src/main.py dist/ACINFOLM/code/
	cp $(ACINFOLM_DIR)/code/config/*.json dist/ACINFOLM/schemas
	cp $(ACINFOLM_DIR)/config/*.json dist/ACINFOLM/configs/
	cp $(ACINFOLM_DIR)/job_configs/*.json dist/ACINFOLM/job_configs/

build_acinfolm_otl:
	$(MAKE) --directory=$(ACINFOLM_OTL_DIR) build
	mkdir -p dist/ACINFOLM_OTL/code dist/ACINFOLM_OTL/configs/ dist/ACINFOLM_OTL/schemas dist/ACINFOLM_OTL/job_configs
	cp $(ACINFOLM_OTL_DIR)/dist/* dist/ACINFOLM_OTL/code/
	cp $(ACINFOLM_OTL_DIR)/code/pythonlib/main/src/main.py dist/ACINFOLM_OTL/code/
	cp $(ACINFOLM_OTL_DIR)/code/config/*.json dist/ACINFOLM_OTL/schemas
	cp $(ACINFOLM_OTL_DIR)/config/*.json dist/ACINFOLM_OTL/configs/
	cp $(ACINFOLM_OTL_DIR)/job_configs/*.json dist/ACINFOLM_OTL/job_configs/

build_acinfotgl:
	$(MAKE) --directory=$(ACINFOTGL_DIR) build
	mkdir -p dist/ACINFOTGL/code dist/ACINFOTGL/configs/ dist/ACINFOTGL/schemas dist/ACINFOTGL/job_configs
	cp $(ACINFOTGL_DIR)/dist/* dist/ACINFOTGL/code/
	cp $(ACINFOTGL_DIR)/code/pythonlib/main/src/main.py dist/ACINFOTGL/code/
	cp $(ACINFOTGL_DIR)/code/config/*.json dist/ACINFOTGL/schemas
	cp $(ACINFOTGL_DIR)/config/*.json dist/ACINFOTGL/configs/
	cp $(ACINFOTGL_DIR)/job_configs/*.json dist/ACINFOTGL/job_configs/

build_acinfotgl_otl:
	$(MAKE) --directory=$(ACINFOTGL_DIR) build
	mkdir -p dist/ACINFOTGL_OTL/code dist/ACINFOTGL_OTL/configs/ dist/ACINFOTGL_OTL/schemas dist/ACINFOTGL_OTL/job_configs
	cp $(ACINFOTGL_DIR)/dist/* dist/ACINFOTGL_OTL/code/
	cp $(ACINFOTGL_DIR)/code/pythonlib/main/src/main.py dist/ACINFOTGL_OTL/code/
	cp $(ACINFOTGL_DIR)/code/config/*.json dist/ACINFOTGL_OTL/schemas
	cp $(ACINFOTGL_DIR)/config/*.json dist/ACINFOTGL_OTL/configs/
	cp $(ACINFOTGL_DIR)/job_configs/*.json dist/ACINFOTGL_OTL/job_configs/