# Linux
FROM ubuntu:22.04

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

RUN apt-get update \
    && apt-get install -y build-essential unzip wget


# Java
RUN apt-get install -y default-jre


# Python
RUN apt-get install -y python3.10 python3-pip \
    && pip3 install --upgrade pip

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt


COPY . .