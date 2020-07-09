# Makefile for majortomo

SHELL := bash

PIP := pip
PIP_SYNC := pip-sync
PIP_COMPILE := pip-compile
PYTHON := python
PYTEST := pytest
PYTHON_VERSION := $(shell $(PYTHON) -c 'import sys; print(sys.version_info[0])')
MYPY_FLAG := $(shell [[ -n "`$(PIP) freeze | grep mypy`" ]] && echo '--mypy')


requirements.txt: requirements.in
	$(PIP_COMPILE) --no-emit-index-url --output-file $@ $<

dev-requirements%.txt: dev-requirements.in
	$(PIP_COMPILE) --no-emit-index-url --output-file $@ $<

.install-dev-requirements%: requirements.txt dev-requirements-py$(PYTHON_VERSION).txt
	$(PIP_SYNC) requirements.txt dev-requirements-py$(PYTHON_VERSION).txt
	$(PIP) freeze > $@

prepare-test: .install-dev-requirements-py$(PYTHON_VERSION)
	$(PIP) install -e .
.PHONY: prepare-test

test: prepare-test
	$(PYTEST) --flake8 --isort $(MYPY_FLAG) majortomo tests
.PHONY: test

wheel:
	$(PIP) install -U pip wheel twine
	$(PYTHON) setup.py bdist_wheel --universal
.PHONY: wheel

requirements: requirements.txt dev-requirements-py$(PYTHON_VERSION).txt
.PHONY: requirements
