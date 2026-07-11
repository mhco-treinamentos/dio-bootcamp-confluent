# Confluent Cloud Troubleshooting Skill

## Skill: `/confluent-troubleshoot`

Debug de problemas comuns nos labs do bootcamp — autenticação, producers/consumers, conectores gerenciados, Schema Registry, Flink e créditos. Comandos `confluent` CLI (v4). Fallback local apenas na seção final.

## Usage

```
/confluent-troubleshoot <issue>
```

### Issues
- `auth` — Login, API keys, ACLs
- `connectivity` — Producer/consumer não conecta
- `connector` — Conector gerenciado FAILED ou sem dados
- `schema` — Erros de compatibilidade no SR
- `lag` — Consumer lag crescendo
- `flink` — Statement com erro ou sem output
- `billing` — Créditos sumindo
- `local` — Stack local (fallback)

---

## Diagnóstico base (sempre começar aqui)

```bash
confluent version                     # CLI v4.x?
confluent environment list            # environment certo?
confluent kafka cluster list          # cluster certo? status UP?
confluent kafka cluster describe <cluster-id>   # endpoint = CC_BOOTSTRAP do .env?
confluent api-key list --resource <cluster-id>  # key existe e pertence ao SA esperado?
```

## 1. Autenticação e ACLs

**Sintomas**: `SASL authentication failed`, `TopicAuthorizationException`.

```bash
# A API key existe e aponta para o recurso certo?
confluent api-key list

# ACLs do service account do lab
confluent kafka acl list --service-account <sa-id>

# Faltou ACL? (least privilege: só o prefixo do lab)
confluent kafka acl create --allow --service-account <sa-id> \
  --operations write,describe --topic "dev.payments." --prefix
```

Checklist:
- [ ] `CC_API_KEY/CC_API_SECRET` no `.env` são do **service account do lab** (não a key de admin)
- [ ] Key criada com `--resource <cluster-id>` do cluster em uso (key de outro cluster falha auth)
- [ ] `security.protocol=SASL_SSL`, `sasl.mechanisms=PLAIN` no client
- [ ] Consumer também precisa de ACL no **consumer group** (`--consumer-group "bootcamp-" --prefix`)

## 2. Conectividade producer/consumer

```bash
# Smoke test independente do código Python
confluent kafka topic produce dev.payments.gen.transactions.created.v1
confluent kafka topic consume dev.payments.gen.transactions.created.v1 --from-beginning
```

- CLI funciona e o Python não → problema no config do client (conferir perfil `KAFKA_PROFILE=cloud` e env vars de [rules.md](../../rules.md))
- Nada funciona → rede corporativa bloqueando 9092/443? Testar `openssl s_client -connect <bootstrap-host>:9092`

## 3. Conector gerenciado

```bash
confluent connect cluster list
confluent connect cluster describe <lcc-id>      # estado + trace por task
confluent connect event describe                 # eventos/logs
```

| Sintoma | Causa provável | Ação |
|---------|----------------|------|
| `FAILED` na criação | Cloud não alcança o Postgres do lab | Túnel/IP público; ver [managed-connector.md](managed-connector.md) |
| `RUNNING` sem mensagens | Snapshot terminou e não há DML novo | Gerar transações no banco fonte |
| Mensagens indo para DLQ | Dado incompatível com schema | `confluent kafka topic consume <dlq> --print-key` e ler headers de erro |
| `FAILED` após funcionar | Slot de replicação removido / senha rotacionada | Recriar slot/atualizar secret + `resume` |

## 4. Schema Registry

**Sintoma**: `409 Conflict — incompatible schema`.

```bash
confluent schema-registry subject describe <subject>          # nível de compatibilidade
confluent schema-registry schema describe --subject <subject> --version latest
```

Correção correta: tornar o campo novo **opcional com default** (BACKWARD). **Não** rebaixar compatibilidade para NONE nos labs — quebrar consumer é exatamente o anti-exemplo da aula. Detalhes → [schema-registry.md](schema-registry.md).

## 5. Consumer lag

```bash
confluent kafka consumer group list
confluent kafka consumer group lag describe <group>
```

Lag crescendo:
1. Consumer vivo? (`describe` mostra membros ativos)
2. Processamento lento → aumentar `max.poll.records`/paralelismo (até nº de partições)
3. Rebalance frequente → `session.timeout.ms=45000`, `max.poll.interval.ms` maior
4. Monitoração contínua via **Metrics API** (módulo 7):

```bash
curl -s https://api.telemetry.confluent.cloud/v2/metrics/cloud/query \
  -u "$CLOUD_API_KEY:$CLOUD_API_SECRET" \
  -H 'Content-Type: application/json' \
  -d '{
    "aggregations": [{"metric": "io.confluent.kafka.server/consumer_lag_offsets"}],
    "filter": {"field": "resource.kafka.id", "op": "EQ", "value": "<cluster-id>"},
    "group_by": ["metric.consumer_group_id"],
    "granularity": "PT1M",
    "intervals": ["PT1H/now"]
  }' | jq
```

## 6. Flink

```bash
confluent flink statement list
confluent flink statement describe <statement-name>
confluent flink statement exception list <statement-name>   # stack trace do statement
```

| Sintoma | Causa | Ação |
|---------|-------|------|
| Statement `FAILED` | SQL inválido / topic-tabela inexistente | `exception list`; conferir nome com backticks |
| Sem output em janela | Watermark não avança (fonte parada) | Gerar eventos contínuos; conferir `watermark` da tabela |
| Output atrasado | Watermark delay alto demais | Ajustar estratégia de watermark do lab |
| Custo subindo | Statement esquecido RUNNING | `confluent flink statement delete` (teardown!) |

## 7. Créditos sumindo (billing)

```bash
# O que está vivo agora?
confluent flink statement list
confluent flink compute-pool list
confluent connect cluster list
confluent kafka cluster list
```

Maiores ofensores: statements Flink 24/7, conectores ociosos, Tableflow habilitado e esquecido. Rodar o `teardown.sh` do lab ([cloud-setup.md](cloud-setup.md)) e conferir **Settings → Billing** na UI.

---

## 8. Fallback local (Docker)

Só topics + Schema Registry existem localmente ([local-dev-fallback.md](local-dev-fallback.md)):

```bash
docker compose ps                                  # healthy?
docker logs broker --tail 50
docker logs schema-registry --tail 50
docker exec broker kafka-broker-api-versions --bootstrap-server localhost:9092
curl -s http://localhost:8081/subjects | jq

# Reset completo (dev only)
docker compose down -v && docker compose up -d
```

| Sintoma local | Correção |
|---------------|----------|
| Broker reiniciando em loop | `docker compose down -v` (CLUSTER_ID/volume inconsistente em KRaft) |
| Client não conecta de fora do container | Usar `localhost:9092` (listener HOST), não `broker:29092` |
| SR 500 na subida | Broker ainda não healthy — aguardar healthcheck |

---

**Última atualização**: 2026-07-11
