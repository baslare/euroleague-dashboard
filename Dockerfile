# syntax=docker/dockerfile:1

FROM python:3.10.6-slim


WORKDIR /euroleague-dashboard

COPY . .

RUN pip3 install -r requirements.txt


