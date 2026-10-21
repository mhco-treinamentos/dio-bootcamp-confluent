# Tratamentos Flink SQL — o antídoto de cada veneno

Statements reutilizáveis pelos labs do módulo 4 (e pelo Desafio Final). Todo SQL aqui é **Flink SQL gerenciado no Confluent Cloud** (compute pool + statements) — topics com schema no Registry aparecem como tabelas automaticamente.

## O pipeline de tratamento

```
gen.transactions.created ──► 01 dedup ──► 02 quality gate ──┬──► clean ──► 03 enrich ──► enriched ──► 05 CEP fraude
        (+ poison pills)                                    └──► dlq (com dlq_reason)      └──► orphans
```

## Mapa veneno → tratamento

| # | Poison pill (reference) | Injetado por | Tratamento | Arquivo |
|---|---|---|---|---|
| 1 | Duplicate / near-duplicate | `PoisonPillInjector` | Dedup por chave de negócio (`ROW_NUMBER`) + state TTL | [01_dedup.sql](01_dedup.sql) |
| 2 | Late-arriving | `PoisonPillInjector` | Watermark calibrada + auditoria de lateness | [00](00_watermarks.sql), [04](04_late_audit.sql) |
| 3 | Out-of-order | natural (partições) | Watermark reordena dentro do delay | [00_watermarks.sql](00_watermarks.sql) |
| 5 | Null/missing fields | `PoisonPillInjector` | Quality gate (`COALESCE`/filtros) | [02_quality_gate.sql](02_quality_gate.sql) |
| 6 | Malformed (semântico) | `PoisonPillInjector` | Quality gate → DLQ Flink com `dlq_reason` | [02_quality_gate.sql](02_quality_gate.sql) |
| 6 | Malformed (serialização) | `poison_producer` | **Não chega ao Flink** — DLQ do Connect (Curso 3, Aula 4) | — |
| 7 | Orphan records | `PoisonPillInjector` | Temporal join `LEFT + IS NULL` → topic de órfãos | [03_enrichment_temporal_join.sql](03_enrichment_temporal_join.sql) |
| 8 | Extreme values | `PoisonPillInjector` | Range check no quality gate | [02_quality_gate.sql](02_quality_gate.sql) |
| 9 | Timezone mismatch | `PoisonPillInjector` | Detector de "evento do futuro" no gate | [02_quality_gate.sql](02_quality_gate.sql) |
| 10 | Retroactive corrections | (labs de sink) | Upsert idempotente no destino — Curso 3, Aula 3 | — |
| 11 | Tombstones | conector CDC (`op=d`) | Changelog semantics / consumer null-safe | rules/sql.md |
| — | Fraude (RULE_VELOCITY) | `TransactionGenerator` (bursts) | `MATCH_RECOGNIZE` + ground truth p/ validar | [05_fraud_velocity.sql](05_fraud_velocity.sql) |

## A lição arquitetural (fio condutor das aulas)

1. **O broker não valida nada** — veneno de serialização entra no topic sem erro; o contrato age na serialização (producer) e no consumo (converter do sink). Demo: `poison_producer.produce_serialization_poison()` → DLQ do Connect com headers `__connect.errors.*`.
2. **DLT é padrão de EDA, não feature do Connect** — o quality gate (02) reimplementa o mesmo desenho em Flink: fluxo bom segue, veneno desviado **com diagnóstico**, nada some em silêncio.
3. **Cada veneno tem seu antídoto em camada diferente** — serialização morre no Connect; semântica morre no Flink; duplicata morre no dedup OU vira upsert inofensivo no destino. Perguntar "onde tratar?" é a pergunta certa da prova.

## Ordem de execução nos labs

`00 → 01 → 02 → 03 → 05` (04 é medidor, roda em paralelo). Todo statement com join/agregação não-janelada declara `SET 'sql.state-ttl'`. Teardown: dropar os statements **e** os topics `dev.payments.flink.*` criados (créditos!).
