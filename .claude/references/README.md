# References — Bootcamp Confluent Cloud 2026

> **Documentação técnica de profundidade. Material complementar consultado pelos agents e pelo autor do bootcamp.**

## Disponíveis

### Plataforma & Pipeline

| Reference | Descrição |
|-----------|-----------|
| `bootcamp-stack-guide.md` | Componentes Confluent Cloud por módulo, pipeline de referência payments/fraude, fallback local e seus limites, disciplina de créditos/teardown |
| `cdc-connect-patterns.md` | CDC com Debezium gerenciado (Postgres 16 como fonte): snapshot modes, envelope, deletes/tombstones, SMTs, DLQ, failure modes (slot growth, schema drift) |

### Kafka & Streaming

| Reference | Descrição |
|-----------|-----------|
| `kafka-producer-guide.md` | Producer para Confluent Cloud (perfil `cloud|local`, SASL_SSL via env vars), semânticas de entrega (RF=3, min.insync.replicas=2, acks=all), rate limiting, callbacks |
| `data-modeling.md` | Padrões de modelagem por domínio, relacionamentos, envelope de eventos |

### Geração de dados sintéticos

| Reference | Descrição |
|-----------|-----------|
| `faker-patterns.md` | Padrões da lib Faker, providers customizados, geração determinística |
| `brazilian-data-formats.md` | CPF, CNPJ, telefones, CEP, chaves Pix, códigos bancários — formatos e validação |
| `behavioral-patterns-guide.md` | Fluxos comportamentais (máquinas de estado, ciclos, sazonalidade, distribuições) |
| `poison-pill-patterns.md` | 12 tipos de problemas de qualidade e estratégias de injeção — exercitados no lab de DLQ (Curso 3, Aula 4) |

## Quando consultar

### Connect & CDC (módulo 3)
- Preparar Postgres 16 para CDC (`wal_level=logical`, REPLICA IDENTITY FULL, publication)
- Configurar o conector Debezium gerenciado (`PostgresCdcSourceV2`) e SMTs (unwrap, RegexRouter)
- Diagnosticar slot growth, schema drift e mensagens no Dead Letter Topic
- Mapear tipos Postgres → Avro (Decimal para valores monetários)

### Kafka & Schema Registry
- Configurar producers idempotentes com o perfil `KAFKA_PROFILE=cloud|local`
- Registrar schemas Avro (`com.bootcamp.payments`) e gerenciar evolução BACKWARD
- Desenhar topics (partições, retention, naming — ver [rules.md](../rules.md))

### Geração de dados
- Construir geradores Faker determinísticos para o domínio de pagamentos
- Aplicar padrões comportamentais (distribuição de horário, geografia, valor, fraude)
- Injetar poison pills para os labs de DLQ e qualidade de dados

### Stack & labs
- Ver `bootcamp-stack-guide.md` para os componentes por módulo e os limites do fallback local
- Aplicar `rules/labs.md` para garantir labs time-boxed, verificáveis e com teardown

---

**Última atualização**: 2026-07-11
