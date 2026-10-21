# Instruction: construir/estender o Desafio Final

> **Playbook RESEARCH → PLAN → ACT → VALIDATE para o pipeline end-to-end de referência em `repo/desafio-final/` — construído camada a camada (build-as-you-go): CDC → Flink → Tableflow → IA → observabilidade. É a implementação de referência que o aluno entrega no padrão DIO.**

## O que é o Desafio Final

Pipeline de detecção de fraude em pagamentos, 100% Confluent Cloud, onde cada camada foi ensinada em um curso:

```
Postgres ──CDC──► Managed Connect ──► Kafka + SR ──► Flink SQL ──► Tableflow/Iceberg
(payments)        (Debezium)          (Curso 2)      (enrichment +   (Curso 5)
                  (Curso 3)                           fraude Curso 4  │
                                          │           + IA Curso 6)   └──► Catálogo
                                          └──► Observabilidade (Metrics API, lag — Curso 7)
```

Regra de ouro: o Desafio Final **compõe** o que os labs construíram — reusa `repo/dataset/` + `repo/schemas/` e referencia os labs; nunca duplica código.

## Pré-requisitos

- Aula 0 concluída ([cloud-setup_v1.md](cloud-setup_v1.md))
- Labs das camadas anteriores à que você vai construir existem (ou suas seções "Replay rápido")
- Geradores em `repo/dataset/generators/` e schemas em `repo/schemas/` prontos

---

## Phase 1: RESEARCH

### 1.1 — Identificar a camada alvo

| Camada | Curso | Entrega | Agent principal |
|--------|-------|---------|-----------------|
| 1. Ingestão + schemas | 2 | Topics + Avro no SR (BACKWARD) + producer cloud | `confluent-cloud-expert` |
| 2. CDC | 3 | Postgres → Debezium gerenciado → topics CDC + DLQ | `connect-cdc-expert` |
| 3. Transformação | 4 | Statements Flink: enrichment, janelas, MATCH_RECOGNIZE | `flink-confluent-expert` |
| 4. Lakehouse | 5 | Tableflow materializando topics enriquecidos como Iceberg | `tableflow-lakehouse-expert` |
| 5. Real-Time AI | 6 | `CREATE MODEL` + `ML_PREDICT` de classificação de fraude | `realtime-ai-expert` |
| 6. Observabilidade | 7 | Metrics API, consumer lag, dashboard, custos | `observability-cost-expert` |

### 1.2 — Levantar o contrato entre camadas
- Que topics/schemas a camada anterior expõe? (`confluent schema-registry subject list`)
- Que garantias a camada seguinte espera? (chave de partição, changelog vs append-only, watermark)
- O que muda no `teardown.sh` agregado?

### 1.3 — Consultar references
- [references/bootcamp-stack-guide.md](../references/bootcamp-stack-guide.md)
- [references/cdc-connect-patterns.md](../references/cdc-connect-patterns.md) (camada 2)
- [rules/sql.md](../rules/sql.md) — watermarks, joins, MATCH_RECOGNIZE, ML_PREDICT (camadas 3 e 5)

---

## Phase 2: PLAN

### 2.1 — Estrutura do diretório

```
repo/desafio-final/
├── README.md              # documento de entrega DIO (ver 2.2)
├── setup.sh               # orquestra os setups por camada (idempotente)
├── teardown.sh            # destrói TUDO, camadas de custo/hora primeiro
├── 01-ingestao/           # topics, schemas, producer
├── 02-cdc/                # seed Postgres + config do conector + DLQ
├── 03-flink/              # sql/ statements numerados na ordem de deploy
├── 04-tableflow/          # habilitação Tableflow + catálogo
├── 05-ai/                 # CREATE MODEL + ML_PREDICT
└── 06-observability/      # queries Metrics API, alertas, dashboard
```

### 2.2 — README como documento de entrega (requisitos DIO)

O README do Desafio Final é avaliado. DEVE documentar:

1. **Decisões de arquitetura** — por que cluster Basic, por que Debezium gerenciado (vs producer direto), por que Flink (vs ksqlDB/Kafka Streams — comparação do módulo 4), por que shift-left (enriquecer antes do Tableflow); cada decisão com alternativa considerada e razão da escolha
2. **Padrões de EDA aplicados** — evento vs comando, pub/sub com consumer groups, ordem por chave, idempotência, semânticas de entrega (at-least-once no producer + idempotence; exactly-once no Flink), DLQ, schema evolution BACKWARD
3. **Trade-offs** — latência vs custo (delay de watermark, tamanho do compute pool), consistência vs disponibilidade, retenção vs armazenamento, regra determinística (MATCH_RECOGNIZE) vs modelo (ML_PREDICT)
4. **Métricas de throughput/latência medidas** — não estimadas: msgs/s do producer, lag do conector CDC, latência end-to-end p50/p95 (evento gerado → fraude detectada), freshness da tabela Iceberg; com a metodologia de medição (Metrics API + timestamps de evento)

Além disso: diagrama da arquitetura, instruções de reprodução (setup → validação → teardown) e custo estimado de rodar tudo por 1h.

### 2.3 — Plano incremental
Uma camada por vez, na ordem dos cursos. Cada camada só entra quando:
- A validação E2E da camada anterior passa
- O `teardown.sh` agregado cobre os recursos novos
- O README ganhou a seção da camada (decisões, trade-offs, métricas)

---

## Phase 3: ACT

### 3.1 — Implementar a camada
Seguir o playbook do lab correspondente ([new-lab_v1.md](new-lab_v1.md) descreve o padrão de seção/validação). Diferenças no Desafio Final:
- Scripts orquestrados pelo `setup.sh` raiz (o aluno sobe o pipeline inteiro com um comando)
- Nomes de recurso prefixados `desafio-` (ex.: conector `desafio-pg-cdc`) para o teardown distinguir do que é lab

### 3.2 — Conectar ao contrato da camada anterior
Exemplo (camada 3 lendo camada 2):
```sql
-- Statement lê o topic CDC do módulo 3 como changelog (upsert)
-- e emite append-only enriquecido para o Tableflow (shift-left)
INSERT INTO `dev.payments.flink.transactions.enriched.v1`
SELECT t.transaction_id, t.amount, t.mcc, a.uf AS account_uf,
       (t.uf <> a.uf) AS uf_mismatch, t.event_time
FROM `dev.payments.pg-payments.transactions.cdc.v1` t
JOIN `dev.payments.pg-payments.accounts.cdc.v1` FOR SYSTEM_TIME AS OF t.event_time AS a
  ON t.account_id = a.account_id;
```

### 3.3 — Medir as métricas da entrega
Instrumentar ao construir, não depois:
```bash
# Throughput do topic (Metrics API)
curl -s -u "$CC_API_KEY:$CC_API_SECRET" \
  "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query" \
  -H 'Content-Type: application/json' -d @06-observability/queries/received-bytes.json | jq .

# Consumer lag do pipeline
confluent kafka consumer group lag describe <group> --cluster $CC_CLUSTER_ID
```
Latência end-to-end: comparar `event_time` (gerador) com o timestamp de chegada em `fraud.detected` — script em `06-observability/scripts/e2e_latency.py`.

### 3.4 — Atualizar README + CHANGELOG
- Seção da camada no README (decisões, EDA patterns, trade-offs, métricas medidas)
- CHANGELOG: `desafio-final: Adiciona camada de <X> ... (@author)`

---

## Phase 4: VALIDATE

### 4.1 — E2E da camada nova
Rodar o fluxo desde o gerador até a saída da camada, com output esperado literal (regra [rules/labs.md](../rules/labs.md)).

### 4.2 — E2E completo (regressão)
Após qualquer camada: subir tudo com `setup.sh`, injetar transações com padrão de fraude conhecido (`seed=42`), verificar:
```bash
confluent kafka topic consume dev.payments.flink.fraud.detected.v1 -b --cluster $CC_CLUSTER_ID
# Esperado: eventos RULE_VELOCITY para o cartão card-frd-001 (padrão plantado pelo seed)
```

### 4.3 — Métricas registradas no README
- [ ] Throughput medido (msgs/s) com data e configuração
- [ ] Latência p50/p95 end-to-end medida
- [ ] Lag do conector CDC em steady state
- [ ] Custo/hora observado do pipeline completo

### 4.4 — Requisitos DIO cobertos
- [ ] Decisões de arquitetura com alternativas e justificativas
- [ ] Padrões de EDA nomeados e apontando para o código que os implementa
- [ ] Trade-offs explícitos
- [ ] Diagrama + instruções de reprodução + custo estimado

### 4.5 — Teardown total
`teardown.sh` raiz destrói as 6 camadas (custo/hora primeiro: compute pool, conectores, Tableflow; depois topics). Verificação pós-teardown incluída.

---

## Checklist final

- [ ] Camada implementada compondo `repo/dataset/` + `repo/schemas/` (zero duplicação dos labs)
- [ ] Contrato com a camada anterior validado (schema, chave, semântica changelog/append)
- [ ] `setup.sh`/`teardown.sh` raiz atualizados e idempotentes
- [ ] E2E completo passa com fraude plantada detectada
- [ ] README de entrega atualizado: decisões, EDA patterns, trade-offs, métricas MEDIDAS
- [ ] CHANGELOG com prefixo `desafio-final:`

---

## Exemplo de uso

```
"Siga desafio-final para adicionar a camada 5 (Real-Time AI):
CREATE MODEL + ML_PREDICT classificando as transações já sinalizadas
por MATCH_RECOGNIZE, com métricas de latência no README"
```

---

**Última atualização**: 2026-07-11
