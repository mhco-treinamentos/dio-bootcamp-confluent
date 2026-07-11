# Instructions — data-streaming-tour

> **Playbooks passo a passo para tarefas recorrentes no workshop (geradores, setup Kafka local, novas cenas, extensões de domínio).**

## Disponíveis

| Instruction | Descrição |
|-------------|-----------|
| [new-generator_v1.md](new-generator_v1.md) | Criar um novo gerador de dados sintéticos para uma entidade |
| [kafka-setup_v1.md](kafka-setup_v1.md) | Subir a stack local (Docker Compose: Kafka + SR + Connect + Oracle XE) |
| [domain-extension_v1.md](domain-extension_v1.md) | Adicionar um novo domínio Oracle (CDC connector + topics + Avro + Flink + Iceberg) |
| [new-scenario_v1.md](new-scenario_v1.md) | Criar uma nova cena E2E com fixtures, comandos e assertions |

## Como usar

```
"Siga new-generator para criar um gerador de Transacao"
"Siga kafka-setup para subir a stack local em data-streaming-tour-2026/"
"Siga domain-extension para adicionar a tabela ESTORNOS no Oracle XE"
"Siga new-scenario para criar uma cena de detecção de fraude por velocidade"
```

## Fluxo workflow

Toda instruction segue RESEARCH → PLAN → ACT → VALIDATE (ver [../workflows/README.md](../workflows/README.md)):

1. **RESEARCH** — Coletar requisitos, consultar [references/](../references/), engajar [agents/](../agents/) relevantes
2. **PLAN** — Desenhar solução, todo list, confirmar approach com o usuário
3. **ACT** — Implementar: schema Avro, gerador, CDC, Flink SQL, Iceberg sink, testes
4. **VALIDATE** — Rodar pytest, validar schemas via hook PII, cronometrar a cena
