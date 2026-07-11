# Workflows — data-streaming-tour

> **Sistema RESEARCH → PLAN → ACT → VALIDATE para construir cenas e geradores do workshop.**

## Visão geral

Toda tarefa não-trivial segue este fluxo de 4 fases:

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ RESEARCH │ ─► │   PLAN   │ ─► │   ACT    │ ─► │ VALIDATE │
└──────────┘    └──────────┘    └──────────┘    └──────────┘
     │               │               │               │
     ▼               ▼               ▼               ▼
 • References    • Todo list     • Gerador       • Tests
 • Agents        • Arquitetura   • CDC config    • Schema check
 • Pesquisa      • Aprovação     • Flink SQL     • E2E (cena
                                 • Iceberg sink    completa)
```

## Fases

### RESEARCH ([research.md](research.md))
- Coletar requisitos da cena (objetivo pedagógico, duração alvo, pré-requisitos)
- Consultar [references/](../references/) — Oracle CDC, formatos brasileiros, padrões Faker
- Engajar agents de domínio (`oracle-cdc-expert`, `flink-self-managed-expert`, `iceberg-expert`, `trino-expert`)
- Inspecionar topics, schemas e estrutura de cenas anteriores antes de propor novas

### PLAN ([plan.md](plan.md))
- Desenhar a solução (topology, watermarks, TTL, particionamento Iceberg)
- Criar todo list e capturar riscos (tempo da cena, RAM, falhas comuns)
- Obter aprovação do usuário em decisões de design
- Definir critério de sucesso e fluxo de recuperação (cleanup commands)

### ACT ([act.md](act.md))
- Implementar geradores Python para payments (em `<edicao>/shared/generators/payments/`)
- Criar schemas Avro com namespace `com.workshop.payments.*` e tags PII/PCI
- Configurar Oracle CDC (supplemental logging, Debezium config, SMTs)
- Construir jobs Flink SQL (watermark, state TTL, checkpoint)
- Escrever Iceberg sink + queries Trino para validação
- Escrever testes pytest + fixtures determinísticas (`seed=42`)
- Documentar a cena: objetivo, comandos, output esperado, validação, cleanup

### VALIDATE ([validate.md](validate.md))
- Rodar pytest com coverage (≥80% em código compartilhado da edição)
- Validar compatibilidade Avro (BACKWARD)
- Testar latência end-to-end e exactly-once
- Verificar conformidade LGPD/PCI-DSS (Avro tags via hook `validate-avro-pii.sh`)
- Cronometrar a cena real para confirmar caber no budget (regra em `rules/workshop.md`)

## Quick tasks

Tarefas simples (< 3 passos) pulam o workflow formal:
- Adição de um único field em schema existente
- Bug fix isolado em um arquivo
- Ajustes pequenos de config (partição, TTL, retention)

Ver [quick-tasks.md](quick-tasks.md).
