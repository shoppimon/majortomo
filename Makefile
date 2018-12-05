# Makefile for majortomo

SHELL := bash

PIP := pip
PYTHON := python
PYTEST := pytest

MYPY_FLAG := '--mypy'

test:
	$(PIP) install -r requirements-test.txt -e .
	$(PYTEST) --flake8 --isort $(MYPY_FLAG) majortomo tests

wheel:
	$(PYTHON) setup.py bdist_wheel --universal
