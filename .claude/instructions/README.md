# Instructions — Bootcamp Confluent Cloud 2026

> **Playbooks passo a passo para tarefas recorrentes no bootcamp (setup cloud, geradores, novos labs, extensões de domínio, desafio final).**

## Disponíveis

| Instruction | Descrição |
|-------------|-----------|
| [cloud-setup_v1.md](cloud-setup_v1.md) | Aula 0: CLI `confluent`, environment, cluster, API keys, teardown |
| [new-generator_v1.md](new-generator_v1.md) | Criar um novo gerador de dados sintéticos para uma entidade |
| [new-lab_v1.md](new-lab_v1.md) | Criar um novo lab em `repo/labs/modulo-0X-*/` com setup/teardown e comandos verificáveis |
| [domain-extension_v1.md](domain-extension_v1.md) | Adicionar uma nova entidade (Postgres CDC + topics + Avro + Flink + Tableflow) |
| [desafio-final_v1.md](desafio-final_v1.md) | Construir/estender o pipeline de referência em `repo/desafio-final/` camada a camada |

## Como usar

```
"Siga cloud-setup para provisionar o ambiente da Aula 0"
"Siga new-generator para criar um gerador de Transaction"
"Siga new-lab para criar o lab de DLQ do módulo 3"
"Siga domain-extension para adicionar a tabela refunds no Postgres fonte"
"Siga desafio-final para adicionar a camada de observabilidade ao pipeline"
```

## Fluxo workflow

Toda instruction segue RESEARCH → PLAN → ACT → VALIDATE (ver [../workflows/README.md](../workflows/README.md)):

1. **RESEARCH** — Coletar requisitos, consultar [references/](../references/), engajar [agents/](../agents/) relevantes
2. **PLAN** — Desenhar solução, todo list, confirmar approach com o usuário
3. **ACT** — Implementar: schema Avro, gerador, CDC, Flink SQL, Tableflow, testes
4. **VALIDATE** — Rodar pytest, validar schemas via hook PII, testar comandos do lab ponta a ponta, executar teardown
