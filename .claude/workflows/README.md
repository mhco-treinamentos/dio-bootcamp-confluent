# Workflows — Bootcamp Confluent Cloud 2026

> **Sistema RESEARCH → PLAN → ACT → VALIDATE para construir labs e geradores do bootcamp.**

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
 • Pesquisa      • Aprovação     • Flink SQL     • E2E (lab
                                 • Tableflow       completo)
```

## Fases

### RESEARCH ([research.md](research.md))
- Coletar requisitos do lab (objetivo pedagógico, duração alvo, pré-requisitos)
- Consultar [references/](../references/) — CDC/Connect, formatos brasileiros, padrões Faker
- Engajar agents de domínio (`connect-cdc-expert`, `flink-confluent-expert`, `tableflow-lakehouse-expert`, `realtime-ai-expert`)
- Inspecionar topics, schemas e estrutura de labs anteriores antes de propor novos

### PLAN ([plan.md](plan.md))
- Desenhar a solução (topology, watermarks, TTL, particionamento)
- Criar todo list e capturar riscos (tempo do lab, custo de créditos, falhas comuns)
- Obter aprovação do usuário em decisões de design
- Definir critério de sucesso e fluxo de recuperação (teardown commands)

### ACT ([act.md](act.md))
- Implementar geradores Python para payments (em `repo/shared/generators/payments/`)
- Criar schemas Avro com namespace `com.bootcamp.payments.*` e tags PII/PCI
- Configurar CDC (Postgres fonte, connector Debezium gerenciado, SMTs)
- Construir statements Flink SQL (watermark, state TTL, janelas)
- Habilitar Tableflow + validar consumo via catálogo
- Escrever testes pytest + fixtures determinísticas (`seed=42`)
- Documentar o lab: objetivo, comandos, output esperado, validação, teardown

### VALIDATE ([validate.md](validate.md))
- Rodar pytest com coverage (≥80% em código compartilhado)
- Validar compatibilidade Avro (BACKWARD)
- Testar latência end-to-end e semânticas de entrega
- Verificar conformidade LGPD/PCI-DSS (Avro tags via hook `validate-avro-pii.sh`)
- Cronometrar o lab real para confirmar caber no budget (regra em `rules/labs.md`)
- Executar teardown e confirmar que nenhum recurso cloud ficou ativo

## Quick tasks

Tarefas simples (< 3 passos) pulam o workflow formal:
- Adição de um único field em schema existente
- Bug fix isolado em um arquivo
- Ajustes pequenos de config (partição, TTL, retention)

Ver [quick-tasks.md](quick-tasks.md).
