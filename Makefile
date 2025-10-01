# ----- GRIB XML/RECON PIPELINE -----

# Allow overriding the Python interpreter: `make xml-all PY=python3`
PY ?= python
DATA_DIR := data
OUTDIR := output_xml
CONVERT := src/weather/convert_grb.py
RECON   := src/weather/reconstruct_xml.py

.PHONY: xml-all reconstruct-all xml-small xml-large recon-small recon-large verify-large verify-small

xml-all:
	$(PY) $(CONVERT) --in $(DATA_DIR)/small_subset_500mb.grb2 $(DATA_DIR)/large_full_1deg.grb2 --outdir $(OUTDIR)

reconstruct-all:
	$(PY) $(RECON) --in $(DATA_DIR)/small_subset_500mb.grb2 $(DATA_DIR)/large_full_1deg.grb2

# Convenience single-file targets
xml-small:
	$(PY) $(CONVERT) --in $(DATA_DIR)/small_subset_500mb.grb2 --outdir $(OUTDIR)

xml-large:
	$(PY) $(CONVERT) --in $(DATA_DIR)/large_full_1deg.grb2 --outdir $(OUTDIR)

recon-small:
	$(PY) $(RECON) --in $(DATA_DIR)/small_subset_500mb.grb2

recon-large:
	$(PY) $(RECON) --in $(DATA_DIR)/large_full_1deg.grb2

verify-large:
	$(PY) tools/verify_roundtrip.py $(DATA_DIR)/large_full_1deg.grb2 $(DATA_DIR)/reconstructed_large_full_1deg.grb2

verify-small:
	$(PY) tools/verify_roundtrip.py $(DATA_DIR)/small_subset_500mb.grb2 $(DATA_DIR)/reconstructed_small_subset_500mb.grb2