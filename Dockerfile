FROM python:3-slim

COPY . .

RUN pip install pip --upgrade
RUN pip install -r requirements.txt
