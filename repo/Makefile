# Bootcamp Confluent Cloud 2026 — roteiro de gravação executável.
#
# Cada alvo é uma CENA da demo: você grava executando um comando por vez.
# Alvos com sufixo -a / -b rodam em TERMINAIS DIFERENTES (indicado no help).
#
#   make help          # lista tudo
#   make up            # sobe o sandbox local (Kafka KRaft + Schema Registry)
#   make aula2-cena1   # primeira cena da Aula 2

SHELL := /bin/bash
PY := .venv/bin/python
COMPOSE := docker compose -f deployment/docker/docker-compose.yml
KAFKA := docker exec bootcamp-kafka
BOOTSTRAP := localhost:9092

DEMO2 := $(PY) -m demos.curso1.aula2_fila_vs_streaming
DEMO3 := $(PY) -m demos.curso1.aula3_anatomia_do_log
DEMO4 := $(PY) -m demos.curso1.aula4_grupos_e_rebalance
DEMO5 := $(PY) -m demos.curso1.aula5_ordem_e_idempotencia
API4P := $(PY) -m demos.curso1.aula4_api_producer
API4C := $(PY) -m demos.curso1.aula4_api_consumer
API5 := $(PY) -m demos.curso1.aula5_api_entrega

.DEFAULT_GOAL := help
.PHONY: help up down logs status install test lint fmt check seed

# ============================================================================
# Ambiente
# ============================================================================

help:  ## Lista os alvos disponíveis
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[96m%-22s\033[0m %s\n", $$1, $$2}'

up:  ## Sobe o sandbox local (Kafka KRaft + Schema Registry)
	$(COMPOSE) up -d
	@echo "aguardando o broker ficar saudável..."
	@until $(KAFKA) kafka-broker-api-versions --bootstrap-server $(BOOTSTRAP) >/dev/null 2>&1; \
		do sleep 2; done
	@echo "✓ sandbox pronto — bootstrap em $(BOOTSTRAP), Schema Registry em :8081"

down:  ## Derruba o sandbox e APAGA os volumes (reset total)
	$(COMPOSE) down -v

status:  ## Estado dos containers e lista de topics
	@$(COMPOSE) ps
	@echo && echo "topics:"
	@$(KAFKA) kafka-topics --list --bootstrap-server $(BOOTSTRAP) 2>/dev/null || true

logs:  ## Segue os logs do broker
	$(COMPOSE) logs -f kafka

# ============================================================================
# Desenvolvimento
# ============================================================================

install:  ## Cria o venv e instala as dependências
	python3.11 -m venv .venv
	.venv/bin/pip install -q --upgrade pip
	.venv/bin/pip install -q -e ".[dev,cdc]"

test:  ## Roda a suíte de testes
	$(PY) -m pytest tests/ -q

lint:  ## Verifica lint e formatação
	.venv/bin/ruff check .
	.venv/bin/black --check .
	.venv/bin/mypy dataset/ producers/ consumers/

fmt:  ## Formata o código
	.venv/bin/ruff check . --fix
	.venv/bin/black .

check: lint test  ## lint + testes (o que roda antes de commitar)

seed:  ## Regenera o seed determinístico do Neon (seed=42)
	$(PY) -m dataset.neon.generate_seed

# ============================================================================
# CURSO 1 · AULA 2 — Message Queue vs Data Streaming
# ============================================================================
.PHONY: aula2-setup aula2-cena1a aula2-cena1b aula2-cena2a aula2-cena2b aula2-cena3 aula2-teardown

aula2-setup:  ## [Aula 2] Cria o topic da demo
	$(DEMO2) setup

aula2-cena1a:  ## [Aula 2 · FILA] TERMINAL 1: consumidor A no grupo app-pagamentos
	$(DEMO2) consume --group app-pagamentos --label consumidor-A --color 0

aula2-cena1b:  ## [Aula 2 · FILA] TERMINAL 2: consumidor B no MESMO grupo
	$(DEMO2) consume --group app-pagamentos --label consumidor-B --color 1

aula2-produce:  ## [Aula 2] TERMINAL 3: publica 10 transações
	$(DEMO2) produce --count 10

aula2-cena2a:  ## [Aula 2 · STREAMING] TERMINAL 1: consumidor A no grupo antifraude
	$(DEMO2) consume --group antifraude --label antifraude --color 0

aula2-cena2b:  ## [Aula 2 · STREAMING] TERMINAL 2: consumidor B no grupo analytics
	$(DEMO2) consume --group analytics --label analytics --color 1

aula2-cena3:  ## [Aula 2 · REPLAY] Consumidor novo lendo desde o início
	$(DEMO2) consume --group auditoria-nova --label auditoria --color 2 --from-beginning

aula2-teardown:  ## [Aula 2] Remove o topic
	$(DEMO2) teardown

# ============================================================================
# CURSO 1 · AULA 3 — O log do Kafka
# ============================================================================
.PHONY: aula3-cena1 aula3-cena2 aula3-cena3 aula3-cena4 aula3-cena5 aula3-teardown

aula3-cena1:  ## [Aula 3] Cria topic com 3 partições e descreve
	$(DEMO3) setup
	@echo
	$(KAFKA) kafka-topics --describe \
		--topic lab.payments.transactions.log.v1 --bootstrap-server $(BOOTSTRAP)

aula3-cena2:  ## [Aula 3] Produz 9 msgs SEM chave e mostra partição/offset/timestamp
	$(DEMO3) produce --count 9
	$(DEMO3) consume --label leitura-1

aula3-cena3:  ## [Aula 3] Espia os segments no disco do broker
	@echo "arquivos da partição 0 no disco do broker:"
	$(KAFKA) ls -lh /var/lib/kafka/data/lab.payments.transactions.log.v1-0/

aula3-cena4:  ## [Aula 3] Relê a mesma partição — log imutável
	@echo "=== 1ª leitura ==="
	$(DEMO3) consume --label releitura-a
	@echo "=== 2ª leitura (mesmos offsets) ==="
	$(DEMO3) consume --label releitura-b

aula3-cena5:  ## [Aula 3] Offsets finais — o tamanho de cada partição
	$(DEMO3) offsets

aula3-teardown:  ## [Aula 3] Remove o topic
	$(DEMO3) teardown

# ============================================================================
# CURSO 1 · AULA 4 — Producers, Consumers e rebalance
# ============================================================================
.PHONY: aula4-setup aula4-producer aula4-consumer-a aula4-consumer-b aula4-lag aula4-teardown

aula4-setup:  ## [Aula 4] Cria o topic da demo
	$(DEMO4) setup

aula4-producer:  ## [Aula 4] TERMINAL 1: producer lento (1 msg/s) — deixe rodando
	$(DEMO4) produce --rate 1

aula4-consumer-a:  ## [Aula 4] TERMINAL 2: consumidor A (sozinho → pega as 3 partições)
	$(DEMO4) consume --label consumidor-A --color 0

aula4-consumer-b:  ## [Aula 4] TERMINAL 3: consumidor B (entra e dispara o rebalance)
	$(DEMO4) consume --label consumidor-B --color 1

aula4-lag:  ## [Aula 4] TERMINAL 4: current offset, log end offset e LAG
	$(DEMO4) lag

aula4-teardown:  ## [Aula 4] Remove o topic
	$(DEMO4) teardown

# ============================================================================
# CURSO 1 · AULA 5 — Ordem, idempotência e entrega
# ============================================================================
.PHONY: aula5-setup aula5-cena1 aula5-cena2 aula5-cena3 aula5-teardown

aula5-setup:  ## [Aula 5] Cria o topic da demo
	$(DEMO5) setup

aula5-cena1:  ## [Aula 5] Ordem por chave: produz ciclo de vida e mostra a ordem
	$(DEMO5) produce
	$(DEMO5) ordem

aula5-cena2:  ## [Aula 5] at-least-once cru — RODE DUAS VEZES para ver a duplicata
	$(DEMO5) crash

aula5-cena3:  ## [Aula 5] Consumidor idempotente — duplicata inofensiva
	$(DEMO5) idempotente

aula5-teardown:  ## [Aula 5] Remove o topic
	$(DEMO5) teardown

# ============================================================================
# CURSO 1 · A API POR DENTRO (Aulas 4-5) — anatomia de producer e consumer
# ============================================================================
.PHONY: aula4-api-producer aula4-api-envios aula4-api-acks aula4-api-consumer-a aula4-api-consumer-b aula4-api-commits aula4-api-reset aula4-api-assign aula4-api-lag aula4-api-teardown
.PHONY: aula5-api-ordem aula5-api-sem-chave aula5-api-crash-antes aula5-api-crash-depois
.PHONY: aula5-api-dedup aula5-api-teardown

aula4-api-producer:  ## [Aula 4·API] Producer campo a campo: produce assíncrono, callback, flush
	$(API4P)

aula4-api-consumer-a:  ## [Aula 4·API] Consumer A — subscribe, rebalance visível, commit manual
	$(API4C) --nome A

aula4-api-consumer-b:  ## [Aula 4·API] Consumer B — suba no 2º terminal e veja o rebalance
	$(API4C) --nome B

aula4-api-commits:  ## [Aula 4·API] Auto vs manual síncrono vs assíncrono, cronometrados
	$(API4C) --cena commits

aula4-api-reset:  ## [Aula 4·API] auto.offset.reset: earliest lê o histórico, latest só o futuro
	$(API4C) --cena reset

aula4-api-assign:  ## [Aula 4·API] Leitura manual de partição/offset — sem grupo, sem rebalance
	$(API4C) --cena assign

aula4-api-lag:  ## [Aula 4·API] CURRENT-OFFSET / LOG-END-OFFSET / LAG do grupo, via API
	$(API4C) --cena lag

aula4-api-envios:  ## [Aula 4·API] Fire-and-forget vs síncrono vs assíncrono, cronometrados
	$(API4P) --cena envios

aula4-api-acks:  ## [Aula 4·API] A escada acks=0|1|all — e por que idempotência exige all
	$(API4P) --cena acks

aula4-api-teardown:  ## [Aula 4·API] Remove o topic da demo de API
	$(API4P) --teardown

aula5-api-ordem:  ## [Aula 5·API] Chave → partição no delivery report
	$(API5) ordem

aula5-api-sem-chave:  ## [Aula 5·API] A ordem quebrada: mesmo ciclo, sem chave
	$(API5) ordem --sem-chave

aula5-api-crash-antes:  ## [Aula 5·API] Commit ANTES + crash — rode 2x: mensagem PERDIDA
	$(API5) crash --commit antes

aula5-api-crash-depois:  ## [Aula 5·API] Commit DEPOIS + crash — rode 2x: mensagem DUPLICADA
	$(API5) crash --commit depois

aula5-api-dedup:  ## [Aula 5·API] Duplicata inofensiva com set de ids
	$(API5) dedup

aula5-api-teardown:  ## [Aula 5·API] Remove os topics da demo de API
	$(API5) teardown

# ============================================================================
# Curso 1 completo
# ============================================================================
.PHONY: curso1-teardown

curso1-teardown: aula2-teardown aula3-teardown aula4-teardown aula5-teardown aula4-api-teardown aula5-api-teardown  ## Remove TODOS os topics do Curso 1
	@echo "✓ topics do Curso 1 removidos"
