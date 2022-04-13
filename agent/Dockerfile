FROM python:3.8-bullseye

LABEL maintainer="shin@sforzando.co.jp"

ARG PREFECT_PROJECT_NAME=etude-Prefect

ENV WORK_DIR /agent
WORKDIR $WORK_DIR
ENV PREFECT__USER_CONFIG_PATH $WORK_DIR/config.toml

RUN apt-get update && \
    apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release supervisor
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update && \
    apt-get install -y docker-ce docker-ce-cli

RUN pip install --upgrade pip && pip install prefect

COPY ./agent .
COPY ./flow/flows ./flows
COPY ./flow/tasks ./tasks


RUN prefect backend server

#CMD ["prefect", "agent", "local", "start", "--show-flow-logs"]
CMD supervisord --configuration ${WORK_DIR}/supervisord.conf
