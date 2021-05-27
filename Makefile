.PHONY: all environment install install-mac install-linux install-windows lint validate test test-example coverage coverage-html coverage-xml coverage-erase

CODE_COVERAGE ?= 80
WINDOWS_32BIT_GMPY2 ?= packages/gmpy2-2.0.8-cp38-cp38-win32.whl
WINDOWS_64BIT_GMPY2 ?= packages/gmpy2-2.0.8-cp38-cp38-win_amd64.whl
OS ?= $(shell python -c 'import platform; print(platform.system())')
IS_64_BIT ?= $(shell python -c 'from sys import maxsize; print(maxsize > 2**32)')

# Change these if necessary for your local platform; maybe we need to do some kind of autoconf
# to detect these things?

PIPENV = pipenv
PYTHON38 = python3.8
PIP38 = pip3.8

all: environment sys-dependencies install validate lint coverage

requirements.txt: Pipfile
	$(PIP38) freeze > requirements.txt

# Generates the `typings` directory with boto3 stubs, which we commit as
# part of the repository, rather than regenerating every time.
boto3_stubs:
	$(PIP38) install mypy_boto3_builder
	$(PYTHON38) -m mypy_boto3_builder --installed --skip-services typings -d -s s3 ec2

environment:
	@echo 🔧 PIPENV SETUP
	$(PIP38) install pipenv
	$(PIPENV) install --dev

sys-dependencies:
	@echo 📦 Install System Dependencies
	@echo Operating System identified as $(OS)
ifeq ($(OS), Linux)
	make sys-dependencies-linux
endif
ifeq ($(OS), Darwin)
	make sys-dependencies-mac
endif
ifeq ($(OS), Windows)
	make sys-dependencies-windows
endif
ifeq ($(OS), Windows_NT)
	make sys-dependencies-windows
endif


install:
	@echo 📦 Install Module
	@echo Operating System identified as $(OS)
ifeq ($(OS), Linux)
	make install-linux
endif
ifeq ($(OS), Darwin)
	make install-mac
endif
ifeq ($(OS), Windows)
	make install-windows
endif
ifeq ($(OS), Windows_NT)
	make install-windows
endif

sys-dependencies-mac:
	@echo 🍎 MACOS DEPENDENCIES
# gmpy2 requirements
	brew install gmp || true

install-mac:
	@echo 🍎 MACOS INSTALL
	$(PIPENV) run python -m pip install -e .

sys-dependencies-linux:
	@echo 🐧 LINUX DEPENDENCIES
# gmpy2 requirements
	sudo apt-get install libgmp-dev
	sudo apt-get install libmpfr-dev
	sudo apt-get install libmpc-dev

install-linux:
	@echo 🐧 LINUX INSTALL
	$(PIPENV) run python -m pip install -e .

sys-dependencies-windows:
	@echo 🏁 WINDOWS DEPENDENCIES
	# currently nothing?

install-windows:
	@echo 🏁 WINDOWS INSTALL
ifeq ($(IS_64_BIT), True)
	$(PIPENV) run python -m pip install -f $(WINDOWS_64BIT_GMPY2) -e .
endif
ifeq ($(IS_64_BIT), False)
	$(PIPENV) run python -m pip install -f $(WINDOWS_32BIT_GMPY2) -e .
endif

black:
	black apps src tests setup.py

lint:
	@echo 💚 LINT
	@echo 1.Pylint
	$(PIPENV) run pylint .
	@echo 2.Black Formatting
	$(PIPENV) run black --check apps src tests setup.py
	@echo 3.Mypy Static Typing
	$(PIPENV) run mypy apps src tests setup.py
	@echo 4.Package Metadata
	$(PIPENV) run python setup.py check --strict --metadata --restructuredtext
# 	@echo 5.Docstring
# 	pipenv run pydocstyle

auto-lint: black lint

validate: 
	@echo ✅ VALIDATE
	@$(PIPENV) run python -c 'import electionguard; print(electionguard.__package__ + " successfully imported")'

test: 
	@echo ✅ TEST
	$(PIPENV) run pytest . -x

coverage:
	@echo ✅ COVERAGE
	$(PIPENV) run coverage run -m pytest
	$(PIPENV) run coverage report --fail-under=$(CODE_COVERAGE)

coverage-html:
	$(PIPENV) run coverage html -d coverage

coverage-xml:
	$(PIPENV) run coverage xml

coverage-erase:
	@$(PIPENV) run coverage erase

upgrade-electionguard:
	$(PIPENV) uninstall electionguard
	$(PIPENV) install -e 'git+https://github.com/microsoft/electionguard-python.git@feature/generic_chaum_petersen#egg=electionguard'
#	pipenv install -e 'git+https://github.com/microsoft/electionguard-python.git#egg=electionguard'
