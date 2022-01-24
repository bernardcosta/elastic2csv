FROM debian:10-slim
RUN apt-get update
RUN apt-get install -y python3-venv python3-pip
RUN mkdir /usr/src/elastic2csv
WORKDIR /usr/src/elastic2csv
COPY ./elastic2csv/requirements.txt .
RUN pip3 install -r requirements.txt
ENV PYTHONBUFFERED 1

RUN apt install jq -y
RUN apt-get install -y openssh-server
