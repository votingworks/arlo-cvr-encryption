FROM python:3.8.6-buster

RUN apt-get update && apt-get install -y procps make libgmp-dev libmpfr-dev libmpc-dev s3fs libcurl4
RUN pip install virtualenv==20.0.23 pipenv
RUN pip install boto3==1.4.8
RUN pip install -e 'git+https://github.com/votingworks/electionguard-python.git@feature/generic_chaum_petersen#egg=electionguard'
RUN pip install -e 'git+https://github.com/votingworks/arlo-e2e#egg=arlo_e2e'
RUN pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-1.1.0.dev0-cp38-cp38-manylinux1_x86_64.whl

RUN mkdir /mnt/arlo-data

ENV RAY_ENABLE_MULTI_TENANCY=0 RAY_BACKEND_LOG_LEVEL=debug