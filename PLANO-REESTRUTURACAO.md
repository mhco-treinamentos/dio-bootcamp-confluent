# Plano — Reestruturação de pastas do `repo/`

> Status: **aguardando aprovação**. Nenhum arquivo movido ainda.
> Contexto favorável: `repo/` está **100% untracked** no git — sem histórico a preservar, sem `git mv`, sem conflito de merge. Momento ideal para reestruturar.

---

## 1. Estrutura alvo

Suas 6 pastas + 4 irmãs para o que não cabia nelas (decisão: "pastas irmãs"), nomes sem `_api` (decisão: `producers/`/`consumers/`), Flink e Connect dentro de `confluent_cloud/` (decisão: "tudo em confluent_cloud/").

```
repo/
├── README.md                    # mapa do repo (reescrito)
├── pyproject.toml               # packages atualizados
├── .env.example
│
├── deployment/                  # ── INFRA QUE EU PROVISIONO ──────────────
│   ├── docker/                  # fallback local (KRaft + SR + Postgres CDC)
│   │   ├── docker-compose.yml   # NOVO — hoje só existe como doc no .claude
│   │   └── README.md
│   ├── terraform/               # NOVO (esqueleto) — environment, cluster, SA
│   │   └── README.md
│   └── README.md                # fronteira: aqui é infra minha, lá é gerenciado
│
├── dataset/                     # ── OS DADOS ─────────────────────────────
│   ├── generators/              # ← de shared/generators/ (código que produz dados)
│   │   ├── base.py
│   │   ├── common/              # brazilian_formats, temporal
│   │   └── payments/            # customers, accounts, cards, merchants,
│   │                            #   transactions, poison, enums, mcc
│   ├── neon/                    # ← de shared/datasets/neon/ (o banco legado)
│   │   ├── 01_ddl.sql
│   │   ├── 02_seed.sql          # gerado — hash preservado
│   │   ├── 03_cdc_setup.sql
│   │   ├── 99_teardown.sql
│   │   ├── generate_seed.py
│   │   └── README.md
│   └── README.md
│
├── schemas/                     # ── OS CONTRATOS ─────────────────────────
│   └── payments/                # ← de shared/schemas/payments/
│       ├── customer.avsc  account.avsc  card.avsc
│       ├── merchant.avsc  transaction.avsc
│       └── README.md            # NOVO — convenção de subject + tags PII/PCI
│
├── producers/                   # ── DO SIMPLES AO ROBUSTO ────────────────
│   ├── 01_hello_kafka.py        # NOVO — Curso 2 Aula 4: 10 linhas, string, sem SR
│   ├── 02_json_producer.py      # NOVO — dict → JSON, callback de entrega
│   ├── 03_avro_producer.py      # NOVO — + Schema Registry, contrato na ingestão
│   ├── 04_reliable_producer.py  # ← de shared/producers/kafka_producer.py
│   │                            #   (idempotência, acks=all, métricas, profile)
│   ├── 05_poison_producer.py    # ← de shared/producers/poison_producer.py
│   ├── config.py                # NOVO — build_producer_config extraído (reuso)
│   └── README.md                # a progressão explicada aula a aula
│
├── consumers/                   # ── DO SIMPLES AO ROBUSTO ────────────────
│   ├── 01_hello_consumer.py     # NOVO — poll loop mínimo, auto-commit
│   ├── 02_json_consumer.py      # NOVO — desserialização + tratamento de erro
│   ├── 03_avro_consumer.py      # NOVO — + Schema Registry
│   ├── 04_reliable_consumer.py  # NOVO — manual commit, at-least-once, rebalance
│   ├── 05_dlq_consumer.py       # NOVO — try/retry/desvia p/ DLT (padrão da Aula 4)
│   ├── config.py                # NOVO — build_consumer_config
│   └── README.md
│
├── confluent_cloud/             # ── O QUE RODA NO CONFLUENT CLOUD ────────
│   ├── cli/                     # NOVO — scripts confluent CLI
│   │   ├── create_topics.sh
│   │   ├── service_accounts.sh
│   │   └── teardown.sh
│   ├── connect/                 # NOVO — configs de conectores gerenciados
│   │   ├── pg-payments-cdc.json         # Debezium → Neon
│   │   ├── pg-analytics-sink.json       # sink com upsert + DLQ
│   │   └── README.md
│   ├── flink/                   # ← de shared/flink/ (statements SQL)
│   │   ├── 00_watermarks.sql … 05_fraud_velocity.sql
│   │   └── README.md
│   ├── schema_registry/         # NOVO — registro e checagem de compatibilidade
│   │   ├── register_schemas.sh
│   │   └── README.md
│   └── README.md
│
├── labs/                        # ── modulo-02..07 (build-as-you-go) ──────
│   └── README.md                # NOVO — esqueleto; labs entram depois
│
├── desafio-final/               # ── pipeline de referência ───────────────
│   └── README.md                # NOVO — esqueleto
│
└── tests/                       # ── espelha a nova estrutura ─────────────
    ├── conftest.py
    ├── dataset/
    │   ├── generators/          # ← de tests/generators/
    │   └── neon/                # ← de tests/datasets/
    ├── producers/               # ← de tests/producers/
    └── consumers/               # NOVO
```

**Fronteira `deployment/` × `confluent_cloud/`** (a regra que evita a gaveta):
> `deployment/` = infraestrutura que **eu** provisiono e opero (Docker do fallback local, Terraform que *cria* o environment/cluster).
> `confluent_cloud/` = artefatos que **rodam dentro** do serviço gerenciado (statements Flink, JSON de conector, schemas registrados, comandos CLI do dia a dia).

---

## 2. Mapa de movimentação (arquivo a arquivo)

| Origem | Destino | Tipo |
|---|---|---|
| `shared/generators/base.py` | `dataset/generators/base.py` | move |
| `shared/generators/common/*` | `dataset/generators/common/*` | move |
| `shared/generators/payments/*` (9 arquivos) | `dataset/generators/payments/*` | move |
| `shared/schemas/payments/*.avsc` (5) | `schemas/payments/*.avsc` | move |
| `shared/datasets/neon/*` (7 arquivos) | `dataset/neon/*` | move |
| `shared/flink/*.sql` (6) + README | `confluent_cloud/flink/*` | move |
| `shared/producers/kafka_producer.py` | `producers/04_reliable_producer.py` + `producers/config.py` | move + split |
| `shared/producers/poison_producer.py` | `producers/05_poison_producer.py` | move |
| `tests/generators/*` | `tests/dataset/generators/*` | move |
| `tests/datasets/*` | `tests/dataset/neon/*` | move |
| `tests/producers/*` | `tests/producers/*` | mantém |
| — | `producers/01..03`, `consumers/01..05` | **novo código** |
| — | `deployment/docker/docker-compose.yml` | **novo** |
| — | `confluent_cloud/{cli,connect,schema_registry}/` | **novo** |

### Imports afetados (~30 linhas de `from shared.…`)

```python
# antes                                    # depois
from shared.generators.base            →   from dataset.generators.base
from shared.generators.payments        →   from dataset.generators.payments
from shared.generators.common.…        →   from dataset.generators.common.…
from shared.producers.kafka_producer   →   from producers.config / producers.reliable
from shared.datasets.neon.generate_seed→   from dataset.neon.generate_seed
```

`dataset/generators/payments/_schema.py` tem o path `parents[2]/schemas/payments` — precisa virar `parents[3]/schemas/payments` (schemas saiu de dentro do pacote).

### `pyproject.toml`

```toml
[tool.setuptools]
packages = ["dataset", "producers", "consumers"]

[tool.pytest.ini_options]
addopts = "-v --cov=dataset --cov=producers --cov=consumers --cov-report=term-missing"
```

---

## 3. Sincronizar o `.claude/` (16 arquivos, ~60 referências)

Os paths antigos estão espalhados pela config. Sem isso, as instructions e skills mandam o Claude escrever no lugar errado.

| Arquivo | Refs | O que muda |
|---|---|---|
| `rules.md` | ~14 | Árvore completa de File Organization (linhas 199–227) |
| `instructions/domain-extension_v1.md` | 7 | Paths de generators/schemas/tests + árvore |
| `instructions/new-generator_v1.md` | 5 | Todos os "**Local**:" de generator/test/schema |
| `instructions/desafio-final_v1.md` | 3 | `repo/shared/` → `dataset/`+`schemas/` |
| `agents/lab-architect.md` | 3 | Referências a `shared/` |
| `skills/confluent/schema-registry.md` | 3 | **Comandos CLI executáveis** — `./shared/schemas/…` |
| `rules/python.md` | 2 | "strict em `shared/`" → nova fronteira |
| `rules/labs.md` | 1 | "nunca duplica o que vive em `repo/shared/`" |
| `skills/confluent/topics.md` | 1 | Path de `.avsc` em comando |
| `skills/workflow/compliance-checker.md` | 1 | `grep` em `repo/shared/schemas/` |
| `references/bootcamp-stack-guide.md` | 1 | Path de generators |
| `workflows/README.md` | 1 | Path de generators |
| `README.md` (.claude) | 2 | Árvore de referência |
| `instructions/{README,cloud-setup,new-lab}` | 7 | Só `labs/` e `desafio-final/` — **não mudam** ✓ |

**Hooks não precisam de nada** — os 6 scripts são path-agnósticos (operam sobre `$FILE_PATH`). Confirmado por inspeção.

---

## 4. Ordem de execução

1. **Criar a nova árvore de diretórios** (vazia) + `__init__.py`
2. **Mover** arquivos existentes (`mv` simples — untracked, sem git)
3. **Reescrever imports** nos arquivos movidos (~30 linhas) + corrigir `_schema.py`
4. **Atualizar** `pyproject.toml` (packages, cov, pythonpath)
5. **Rodar a suíte** → 87 testes devem passar sem alteração de lógica
6. **Regerar o seed** → hash `ecdf4377…` deve bater (prova de que nada quebrou)
7. **Escrever o código novo**: producers 01–03, consumers 01–05, configs
8. **Escrever infra nova**: docker-compose, esqueletos de connect/cli/terraform
9. **Testes dos consumers** novos
10. **Sincronizar `.claude/`** (16 arquivos)
11. **READMEs** de cada pasta nova + README raiz do repo
12. **Validar tudo**: pytest, ruff, black, mypy, hash do seed
13. **CHANGELOG**

## 5. Critérios de aceite

- [ ] `pytest` — 87 testes existentes passando + novos dos consumers
- [ ] `ruff check . && black --check . && mypy` — limpos
- [ ] `python -m dataset.neon.generate_seed` → hash **`ecdf4377…` inalterado**
- [ ] Nenhuma string `shared/` sobrando em `repo/` nem em `.claude/`
- [ ] Todo `.avsc` continua passando no hook `validate-avro-pii.sh`
- [ ] Cada pasta nova tem README explicando seu papel e sua fronteira

## 6. Riscos

| Risco | Mitigação |
|---|---|
| Seed mudar de hash (quebra determinismo prometido aos alunos) | Hash conferido no passo 6, antes de qualquer código novo |
| Referência quebrada no `.claude` → Claude escreve no lugar errado depois | Passo 10 + grep final por `shared/` |
| Escopo inflar (10 arquivos novos de producer/consumer) | Podem ir numa segunda leva se você preferir só a mudança estrutural agora |

---

## Pergunta aberta

O plano inclui **escrever os producers 01–03 e consumers 01–05** (a progressão "simples → robusto" que você pediu). São ~10 arquivos novos de código didático.

Alternativa: fazer **só a reestruturação** agora (mover + sincronizar `.claude` + infra), e a progressão de clients numa próxima rodada. Diz qual prefere na aprovação.
