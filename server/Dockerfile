FROM python:3.8-bullseye

RUN apt-get update && \
    apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
RUN echo "deb [arch=arm64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update && \
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose

WORKDIR /app
COPY . .
ENV PREFECT__USER_CONFIG_PATH="/app/config.toml"

RUN pip install --upgrade pip && pip install prefect

RUN prefect backend server

CMD ["prefect", "server", "start"]
