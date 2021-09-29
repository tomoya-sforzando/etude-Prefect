FROM python:3.8-bullseye

LABEL maintainer="shin@sforzando.co.jp"

ARG PREFECT_PROJECT_NAME=etude-Prefect

ENV WORK_DIR /agent
WORKDIR $WORK_DIR
ENV PREFECT__USER_CONFIG_PATH $WORK_DIR/config.toml

RUN apt-get update && \
    apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release tini
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update && \
    apt-get install -y docker-ce docker-ce-cli

RUN pip install --upgrade pip && pip install prefect

COPY . .

RUN prefect backend server

#CMD ["prefect", "agent", "local", "install", "--show-flow-logs"]
CMD ["prefect", "agent", "docker", "start", "--show-flow-logs", "--volume", "/root/.prefect"]
