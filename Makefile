.DEFAULT_GOAL := all
PY ?= .venv/bin/python

.PHONY: build
build:
	maturin develop --release

.PHONY: format
format:
	ruff format weft tests bench
	cargo fmt

.PHONY: lint
lint:
	ruff check weft tests bench
	cargo fmt --check
	cargo clippy --release --all-targets -- -D warnings

.PHONY: test
test:
	cargo test --release --lib
	$(PY) -m pytest tests -q

.PHONY: all
all: build lint test
