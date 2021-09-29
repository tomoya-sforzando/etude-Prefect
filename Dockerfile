FROM python:3.8-bullseye

WORKDIR /app

RUN pip install --upgrade pip && pip install "prefect[dev, templates, viz, aws]"

COPY . .
ENV PREFECT__USER_CONFIG_PATH="/app/config.toml"

RUN prefect backend server

CMD ["python", "flow.py"]
