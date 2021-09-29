# -*- coding: utf-8 -*-

TIMESTAMP := $(shell date +%Y%m%d%H%M%S)

CMD_DOCKER := docker
CMD_DOCKER_COMPOSE := docker-compose
CONTAINER_SHELL := bash
PREVIEW_URL := http://0.0.0.0:8080/
LATEST_CONTAINER := $(shell docker ps -ql)

.PHONY: ps up recreate setup restart shell logs follow open hide reveal build test doc deploy stop down clean prune help

ps: ## 監視
	${CMD_DOCKER_COMPOSE} ps

up: ## 起動
	${CMD_DOCKER_COMPOSE} up --detach --remove-orphans

recreate: down ## 再建
	${CMD_DOCKER_COMPOSE} up --detach --remove-orphans --build --force-recreate

setup: up ## 初回
	echo "TODO: Not Implemented Yet!"

restart: stop up ; ## 再起

shell: up ## 接続
	${CMD_DOCKER_COMPOSE} ${ARGS} ${CONTAINER_SHELL}

logs: ## 記録
	${CMD_DOCKER_COMPOSE} logs

follow: ## 追跡
	${CMD_DOCKER_COMPOSE} logs -f

open: ## 閲覧
	open ${PREVIEW_URL}

hide: ## 秘匿
	git secret hide -v

reveal: ## 暴露
	git secret reveal -v

build: ## 構築
	echo "TODO: Not Implemented Yet!"

test: up ## 試験
	echo "TODO: Not Implemented Yet!"

doc: up ## 文書
	echo "TODO: Not Implemented Yet!"

deploy: ## 配備
	echo "TODO: Not Implemented Yet!"

stop: ## 停止
	${CMD_DOCKER_COMPOSE} stop

down: ## 削除
	${CMD_DOCKER_COMPOSE} down --rmi all --remove-orphans

clean: down ## 掃除
	echo "TODO: Not Implemented Yet!"

prune: down ## 破滅
	${CMD_DOCKER} system prune --all --force --volumes

help: ## 助言
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
