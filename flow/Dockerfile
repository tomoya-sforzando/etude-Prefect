FROM python:3.8-bullseye

LABEL maintainer="shin@sforzando.co.jp"

ARG PREFECT_PROJECT_NAME=etude-Prefect

ENV PYTHONPATH "${PYTHONPATH}:/flow/tasks/"
ENV WORK_DIR /flow
WORKDIR $WORK_DIR
ENV PREFECT__USER_CONFIG_PATH $WORK_DIR/config.toml

RUN pip install --upgrade pip && pip install "prefect[dev, templates, viz, aws]"

COPY . .

RUN prefect backend server

CMD ["python", "main.py"]
