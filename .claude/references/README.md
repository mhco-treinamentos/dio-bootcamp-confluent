# References — data-streaming-tour

> **Documentação técnica de profundidade. Material complementar consultado pelos agents e pelo autor do workshop.**

## Disponíveis

### Plataforma & Infraestrutura

| Reference | Descrição |
|-----------|-----------|
| `oracle-cdc-patterns.md` | Oracle LogMiner, supplemental logging, Debezium em profundidade, mapeamento de tipos |

### Kafka & Streaming

| Reference | Descrição |
|-----------|-----------|
| `kafka-producer-guide.md` | Boas práticas de producer, rate limiting, callbacks de entrega |
| `data-modeling.md` | Padrões de modelagem por domínio, relacionamentos, evolução de schema |

### Geração de dados sintéticos

| Reference | Descrição |
|-----------|-----------|
| `faker-patterns.md` | Padrões da lib Faker, providers customizados |
| `brazilian-data-formats.md` | CPF, CNPJ, CNS, telefones, CEP — formatos e validação |
| `behavioral-patterns-guide.md` | Fluxos comportamentais (ciclos, sazonalidade, distribuições) |
| `poison-pill-patterns.md` | 12 tipos de problemas de qualidade e estratégias de injeção |

### Workshop / Stack lakehouse OSS

| Reference | Descrição |
|-----------|-----------|
| `workshop-stack-guide.md` | Decisões de arquitetura, matriz de versões, mapeamento workshop ↔ produção, fluxo de cenas |

> Referências de Confluent Cloud, AWS e Terraform foram removidas — workshop é 100% Docker self-managed. Histórico recuperável no git se necessário.

## Quando consultar

### Oracle CDC
- Preparar Oracle XE para CDC (supplemental logging, LogMiner)
- Configurar Debezium para `PAYMENTS.CONTAS`, `PAYMENTS.CARTOES`, `PAYMENTS.TRANSACOES`
- Mapear tipos Oracle → Avro
- Diagnosticar CDC lag e gaps de SCN

### Kafka & Schema Registry (local)
- Configurar producers idempotentes
- Registrar schemas Avro e gerenciar evolução BACKWARD
- Desenhar topics (partições, retention, naming)

### Geração de dados
- Construir geradores Faker determinísticos
- Modelar entidades de pagamentos (contas, cartões, transações)
- Aplicar padrões comportamentais (distribuição de horário, geografia, valor)
- Injetar poison pills para cenas de qualidade de dados

### Workshop (cenas)
- Ver `workshop-stack-guide.md` para a matriz de versões e o fluxo conceitual
- Aplicar `rules/workshop.md` para garantir cenas time-boxed e verificáveis
