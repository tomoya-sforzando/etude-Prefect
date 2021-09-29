# -*- coding: utf-8 -*-

.PHONY: open hide reveal start lint test deploy clean help

open: ## 閲覧
	open http://0.0.0.0:8080/

hide: ## 秘匿
	git secret hide -v

reveal: ## 暴露
	git secret reveal -v

start: ## 開始
	docker compose up -d --build --remove-orphans

lint: ## 監査
	echo "TODO: Not Implemented Yet!"

test: ## 試験
	echo "TODO: Not Implemented Yet!"

deploy: ## 配備
	echo "TODO: Not Implemented Yet!"

clean: ## 掃除
	echo "TODO: Not Implemented Yet!"

help: ## 助言
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
