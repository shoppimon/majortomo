# Makefile for majortomo

SHELL := bash

PIP := pip
PIP_SYNC := pip-sync
PIP_COMPILE := pip-compile
PYTHON := python
PYTEST := pytest

MYPY_FLAG := '--mypy'

%.txt: %.in
	$(PIP_COMPILE) --no-index --output-file $@ $<

.install-dev-requirements: requirements.txt dev-requirements.txt
	$(PIP_SYNC) requirements.txt dev-requirements.txt
	$(PIP) freeze > $@

prepare-test: .install-dev-requirements
	$(PIP) install -e .

test: prepare-test
	$(PYTEST) --flake8 --isort $(MYPY_FLAG) majortomo tests

wheel:
	$(PYTHON) setup.py bdist_wheel --universal
