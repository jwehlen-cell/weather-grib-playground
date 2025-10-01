.PHONY: run-small peek-large
run-small:
	. .venv/bin/activate && python3 src/weather/read_small.py
peek-large:
	. .venv/bin/activate && python3 src/weather/read_large.py

