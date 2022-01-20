FROM python:3.7.12-slim-bullseye
RUN mkdir /usr/src/elasticexport
WORKDIR /usr/src/elasticexport
COPY ./elasticexport/requirements.txt .
RUN pip install -r requirements.txt
ENV PYTHONBUFFERED 1
