# Bootcamp Confluent Cloud 2026 — Repo de código

Código entregável do bootcamp: datasets sintéticos, geradores, schemas Avro, producers e labs.
Domínio único do bootcamp: **payments/fraude** (cartões, contas, transações, antifraude — dados 100% sintéticos, formatos brasileiros).

> 📘 **É aluno do bootcamp?** Comece pelos guias passo a passo: **[docs/](docs/README.md)**

## Estrutura

```
repo/
├── docs/                       # 📘 Guias do aluno, aula por aula (comece aqui)
│   └── curso1/aula1/           # um README por aula, com exercícios e gabarito
├── deployment/                 # Infraestrutura
│   ├── docker/                 # sandbox local: Kafka KRaft + Schema Registry
│   └── terraform/              # (Confluent Cloud — Curso 2+)
├── dataset/                    # Dados sintéticos do domínio payments
│   ├── generators/             # Geradores determinísticos (seed=42)
│   │   ├── base.py             # BaseGenerator ABC
│   │   ├── common/             # brazilian_formats (CPF, CNPJ, telefone, CEP)
│   │   └── payments/           # customers, accounts, cards, merchants, transactions
│   └── neon/                   # DDL + seed do Postgres legado (Neon — Curso 3)
├── schemas/payments/           # Schemas Avro (namespace com.bootcamp.payments.*)
├── producers/                  # Progressão: hello → json → avro → reliable
├── consumers/                  # Progressão: hello → json → avro → reliable → DLQ
├── demos/curso1/               # Scripts das demos (roteiro de gravação)
├── confluent_cloud/            # CLI, Connect, Schema Registry e Flink SQL
├── labs/                       # modulo-02..07 (build-as-you-go)
├── desafio-final/              # Pipeline end-to-end de referência
└── tests/                      # pytest + hypothesis (nunca aponta pro Confluent Cloud)
```

## Setup local

```bash
cd repo
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,cdc]"
cp .env.example .env   # preencha com suas credenciais (nunca commitar)
```

## Comandos

```bash
pytest tests/ -v --cov            # testes (geradores não tocam a nuvem)
ruff check . && black --check .   # lint + formato
python -m dataset.neon.generate_seed   # regenera o seed SQL (seed=42)
```

## Regras do projeto

Padrões inegociáveis do projeto:

- Dinheiro sempre `Decimal` / Avro `bytes+decimal` — nunca float
- PAN sempre mascarado (BIN + last4); CVV/PIN proibidos até em dado sintético
- PII taggeada nos schemas (`confluent:tags`: PII/LGPD, PCI/PCI_DSS, FIN)
- Geradores determinísticos: `seed=42` → todo aluno vê os mesmos dados
- Todo recurso cloud criado por lab tem entrada no `teardown.sh`
