FROM python:3.8-bullseye

LABEL maintainer="shin@sforzando.co.jp"

ENV WORK_DIR=/flow
ENV PREFECT__USER_CONFIG_PATH="$WORK_DIR/config.toml"

WORKDIR $WORK_DIR

RUN pip install --upgrade pip && pip install "prefect[dev, templates, viz, aws]"

COPY . .

RUN prefect backend server

CMD ["python", "flow.py"]
