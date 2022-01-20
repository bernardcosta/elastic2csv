FROM python:3.7.12-slim-bullseye
RUN mkdir /usr/src/app
WORKDIR /usr/src/app
COPY ./elasticexport/requirements.txt .
RUN pip install -r requirements.txt
ENV PYTHONBUFFERED 1
