FROM python:3.7.12-slim-bullseye
RUN mkdir /usr/src/elastic2csv
WORKDIR /usr/src/elastic2csv
COPY ./elastic2csv/requirements.txt .
RUN pip install -r requirements.txt
ENV PYTHONBUFFERED 1
