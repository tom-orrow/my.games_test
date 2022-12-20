FROM python:3.10.8-slim

WORKDIR /app

RUN apt-get update && apt-get install -y unzip

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY prepare_data.sh .
RUN chmod +x prepare_data.sh

# copy project
COPY . .