# Docker file for Majortomo ZeroMQ MDP Broker

FROM python:3.7

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && apt-get install -y libzmq3-dev && apt-get clean -y

RUN useradd -m majortomo

COPY majortomo /usr/local/majortomo/majortomo
COPY requirements.txt setup.py VERSION README.md /usr/local/majortomo/

WORKDIR /usr/local/majortomo
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -e .

USER majortomo

ENTRYPOINT ["python", "-m", "majortomo.broker"]
