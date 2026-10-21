# Curso 7 · Aula 2 — Observabilidade: a Metrics API na unha

> **Guia do aluno** · Bloco 3 — Operação
> ⏱ ~40 min · ☁️ **Metrics API é gratuita** · 1 terminal

Tudo que a UI do Confluent Cloud mostra vem de uma API. Nesta aula você consulta essa API direto — e descobre que os números do **seu** pipeline contam uma história que você não esperava.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito os Cursos 1 a 4 | Precisa haver histórico para medir |
| `curl` e `python3` (ou `jq`) | Para as consultas |
| 1 terminal | |

> 💡 **Boa notícia**: a Metrics API **não cobra**. Esta aula é a mais barata do
> bootcamp — você só lê dados que já existem.

---

## O que você vai saber fazer ao final

- Criar uma API key de escopo **Cloud** (a terceira do bootcamp)
- Descobrir quais métricas existem, sem decorar nomes
- Consultar throughput e **consumer lag** — a métrica rainha
- Exportar em formato Prometheus, sem instalar agente
- Ler storage por tópico e identificar o que está pesando

---

## Os conceitos em 90 segundos

**Três fontes de sinal**, com propósitos diferentes:

| Fonte | Responde | Latência |
|---|---|---|
| **Metrics API** | "como está a saúde?" | ~1 min |
| **Audit log** | "quem fez o quê?" | minutos |
| **Status page** | "é problema meu ou deles?" | — |

**A métrica rainha é o consumer lag.** Ela responde a única pergunta que importa em
streaming: *o consumo está acompanhando a produção?*

E o segredo é lê-la **em tendência, não em valor absoluto**:

```
lag = 5.000 estável      → normal (é o buffer de trabalho)
lag = 5.000 e subindo    → INCIDENTE em formação
lag = 200.000 e caindo   → recuperação em curso, não faça nada
```

> 📖 A hierarquia de dashboards e o vocabulário SLI/SLO/SLA estão **na aula gravada**.

---

## Passo 1 — A terceira API key *(~6 min)*

Você já criou duas no bootcamp: uma de **cluster** (Curso 2) e uma de **Schema
Registry** (Curso 3). A Metrics API exige uma terceira, de escopo **Cloud**:

```bash
confluent api-key create --resource cloud --description "Metrics API"
```

**Output esperado**
```
+------------+------------------------------------------------------------------+
| API Key    | SUA-API-KEY                                                 |
| API Secret | cflt-SEU-SECRET-AQUI |
+------------+------------------------------------------------------------------+
```

```bash
export MK="SUA-API-KEY"
export MS="cflt-SEU-SECRET-AQUI"
export CLUSTER=$(confluent kafka cluster list -o json | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['id'])")
```

> 🎬 **Nota de gravação**: a key acima é a do ambiente de gravação (a mesma do output).
> Grave a **sua** — o secret só aparece uma vez, na criação.

**O que observar**: `--resource cloud`, não um `lkc-` ou `lsrc-`. É o mesmo modelo de
escopo do Curso 2 — cada recurso exige a credencial certa.

> 🔴 Esta key vê **métricas de toda a organização**. É credencial de observabilidade,
> **nunca** de producer. Guarde com o mesmo cuidado das outras.

---

## Passo 2 — Descubra as métricas *(~7 min)*

Não decore nomes — **pergunte**:

```bash
curl -s -u "$MK:$MS" \
  "https://api.telemetry.confluent.cloud/v2/metrics/cloud/descriptors/metrics?resource_type=kafka" \
  | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('total:', len(d['data']))
for m in d['data'][:12]: print(' ', m['name'].split('/')[-1])"
```

**Output esperado**
```
total: 78
  active_connection_count
  client_limit_milliseconds
  cluster_active_link_count
  ...
  cluster_load_percent
```

**78 métricas** só para Kafka. As que importam no dia a dia:

| Métrica | Responde |
|---|---|
| `consumer_lag_offsets` | **A rainha** — o consumo acompanha? |
| `received_bytes` / `sent_bytes` | Throughput de entrada/saída |
| `retained_bytes` | **Storage** — vira dinheiro (Aula 4) |
| `active_connection_count` | Quantos clientes conectados |
| `hot_partition_ingress` | Partição desbalanceada |

> 💡 **Este é o comando mais útil da aula.** Nomes de métrica mudam entre versões; o
> endpoint de descriptors responde o que existe **hoje**, na sua conta.

---

## Passo 3 — Throughput do cluster *(~7 min)*

**🖱 Pela Console**: a página **Overview** do cluster mostra o gráfico de produção/consumo — desenhado a partir **desta mesma API**. A demo pode abrir os dois lado a lado: o gráfico e o número cru.

**⌨️ Pela CLI**:

```bash
curl -s -u "$MK:$MS" -H "Content-Type: application/json" \
  https://api.telemetry.confluent.cloud/v2/metrics/cloud/query \
  -d @- <<EOF
{
  "aggregations": [{"metric": "io.confluent.kafka.server/received_bytes"}],
  "filter": {"field": "resource.kafka.id", "op": "EQ", "value": "$CLUSTER"},
  "granularity": "PT1H",
  "intervals": ["PT6H/now"]
}
EOF
```

> 💡 **Dois truques que limpam o comando** (e valem para toda a aula):
> `-d @-` manda o corpo pelo stdin — o heredoc `<<EOF` entrega o JSON **sem escapar
> aspas** e ainda expande o `$CLUSTER`. E `"PT6H/now"` é um intervalo **relativo** —
> "das últimas 6 horas até agora", sem calcular timestamp nenhum.

**Output esperado**
```json
{"data":[
  {"timestamp":"2026-08-31T20:00:00Z","value":1064.0},
  {"timestamp":"2026-08-31T22:00:00Z","value":924730.0},
  {"timestamp":"2026-08-31T23:00:00Z","value":456580.0}]}
```

**O que observar**: o pico de **924 KB às 22h**. Se você rodou o Curso 4 nesse horário,
esse número é o seu CDC e seus statements Flink trabalhando. **É o seu pipeline, medido.**

A anatomia da query:

| Campo | O que faz |
|---|---|
| `aggregations` | Qual métrica |
| `filter` | Qual recurso (`resource.kafka.id`) |
| `granularity` | `PT1M`, `PT1H`, `P1D` |
| `intervals` | Janela ISO-8601: relativa (`PT6H/now`) ou absoluta (`início/fim`) |

---

## Passo 4 — A métrica rainha *(~8 min)*

**🖱 Pela Console**: no cluster, **Consumers** → clique no grupo → o **lag por partição** em tela. O `curl` abaixo devolve a mesma série, pronta para alerta.

**⌨️ Pela CLI**:

```bash
curl -s -u "$MK:$MS" -H "Content-Type: application/json" \
  https://api.telemetry.confluent.cloud/v2/metrics/cloud/query \
  -d @- <<EOF | python3 -c "
import json,sys
d=json.load(sys.stdin)['data']
ult={}
for p in d: ult[p['metric.consumer_group_id']]=p['value']
for g,v in sorted(ult.items()): print(f'  lag {v:8.0f}  {g}')"
{
  "aggregations": [{"metric": "io.confluent.kafka.server/consumer_lag_offsets"}],
  "filter": {"field": "resource.kafka.id", "op": "EQ", "value": "$CLUSTER"},
  "group_by": ["metric.consumer_group_id"],
  "granularity": "PT1H",
  "intervals": ["PT6H/now"]
}
EOF
```

**Output esperado** (os seus grupos vão variar)
```
  lag        0  connect-lcc-22jxy7q
  lag        0  connect-lcc-yoyv9vp
  lag        0  pg-payments.live
  lag     1993  pg-payments.snapshot
```

**O que observar**: o `group_by` quebra o lag **por consumer group** — o python só
guarda o valor mais recente de cada um. Os `connect-lcc-*` são os consumidores internos
dos seus conectores, em dia (lag 0).

E olhe o `pg-payments.snapshot`: **lag 1993, parado**. É um grupo cujo consumidor não
existe mais — consumiu, commitou offset e morreu. O lag congelou ali. Você acabou de
encontrar, no seu próprio cluster, o padrão "zerado/parado de repente" da tabela abaixo.

**Como ler de verdade**: um único ponto não diz nada. O que importa é a **série**:

| Padrão | Diagnóstico |
|---|---|
| Constante | Saudável — é o buffer natural |
| **Subindo** | Consumo não acompanha → **investigue agora** |
| Caindo de um pico | Recuperação — não intervenha |
| Zerado de repente | Suspeito: consumidor morreu? offset foi resetado? |

Compare com o comando do Curso 1:
```bash
confluent kafka consumer group lag list NOME-DO-GRUPO
```

**A diferença**: o CLI dá uma foto **agora**; a Metrics API dá a **série histórica** —
e é isso que permite alertar em tendência.

---

## Passo 5 — Prometheus sem agente *(~7 min)*

O mesmo dado, em formato que qualquer stack de observabilidade entende:

```bash
curl -s -u "$MK:$MS" \
  "https://api.telemetry.confluent.cloud/v2/metrics/cloud/export?resource.kafka.id=$CLUSTER" \
  | grep "retained_bytes" | head -5
```

**Output esperado**
```
# HELP confluent_kafka_server_retained_bytes The current count of bytes retained by the cluster.
# TYPE confluent_kafka_server_retained_bytes gauge
confluent_kafka_server_retained_bytes{kafka_id="lkc-9kwm1py",topic="pg-payments.payments.transactions",kafka_name="bootcamp-basic",} 1650176.0 1787249580000
```

**O que observar**: formato Prometheus nativo, com labels por tópico. **Nenhum agente
instalado, nenhum exporter, nenhuma porta aberta.**

Para integrar de verdade, três linhas de `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: confluent-cloud
    scrape_interval: 60s          # NÃO menos — rate limit da API
    metrics_path: /v2/metrics/cloud/export
    scheme: https
    static_configs:
      - targets: ['api.telemetry.confluent.cloud']
    params:
      resource.kafka.id: ['lkc-xxxxx']
    basic_auth:
      username: SUA_CLOUD_KEY
      password: SEU_CLOUD_SECRET
```

> ⚠️ **`scrape_interval: 60s` não é sugestão.** As métricas são amostradas a cada 60
> segundos e a API tem rate limit. Raspar mais rápido não traz dado novo — só erro 429.

E a regra de alerta que nasce disso:

```yaml
- alert: ConsumerLagHigh
  expr: confluent_kafka_server_consumer_lag_offsets > 10000
  for: 5m                          # o "for" evita alarme por pico momentâneo
  annotations:
    summary: "Lag alto em {{ $labels.consumer_group_id }}"
```

**O que observar**: o `for: 5m` é o que separa alerta útil de ruído. Sem ele, todo
pico de tráfego acorda alguém de madrugada.

---

## Passo 6 — O que está pesando no seu cluster *(~6 min)*

A mesma query dos Passos 3 e 4 — só muda a métrica e o `group_by`:

```bash
curl -s -u "$MK:$MS" -H "Content-Type: application/json" \
  https://api.telemetry.confluent.cloud/v2/metrics/cloud/query \
  -d @- <<EOF | python3 -c "
import json,sys
d=json.load(sys.stdin)['data']
tot={}
for p in d: tot[p['metric.topic']]=max(tot.get(p['metric.topic'],0),p['value'])
for t,v in sorted(tot.items(), key=lambda x:-x[1])[:8]:
    print(f'  {v/1024:9.1f} KB  {t}')"
{
  "aggregations": [{"metric": "io.confluent.kafka.server/retained_bytes"}],
  "filter": {"field": "resource.kafka.id", "op": "EQ", "value": "$CLUSTER"},
  "group_by": ["metric.topic"],
  "granularity": "PT1H",
  "intervals": ["PT1H/now"],
  "limit": 100
}
EOF
```

**Output esperado** (o seu vai variar)
```
     1614.1 KB  pg-payments.payments.transactions
     1243.4 KB  lab.transactions.datagen
      160.4 KB  pg-payments.payments.accounts
       29.6 KB  transacoes_grandes
        6.1 KB  __debezium-heartbeat-lcc-7yq0011.pg-payments
        1.0 KB  pedidos_sink
```

**O que observar**: uma linha por tópico, sem instalar nada — e repare no
`__debezium-heartbeat-*`: um tópico que **você nunca criou**. É subproduto do conector
CDC. No cluster de validação deste bootcamp, os subprodutos (DLQs e heartbeats de
conectores já deletados) chegaram a ser **os maiores tópicos do cluster** — mais que os
dados de negócio. O aviso da Aula 4 do Curso 3 (*"DLT sem reprocesso vira aterro
sanitário"*) medido em bytes.

Este número vira dinheiro na [Aula 4](../aula4/README.md) — onde você fará a limpeza.

---

## Passo 7 — Fixe os conceitos *(~5 min)*

1. Por que ler lag em tendência em vez de valor absoluto?
2. Sua key de cluster funciona na Metrics API?
3. Por que `scrape_interval` menor que 60s é desperdício?
4. O lag zerou de repente. Boa notícia?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. Porque o valor absoluto não tem significado universal.** Lag de 50.000 pode ser
normal num tópico de 100k msg/s e catastrófico num de 10 msg/s. O que sempre significa
problema é **lag subindo de forma sustentada** — indica que o consumo não acompanha a
produção, e a distância só cresce.

**2. Não.** Metrics API exige key de escopo **`cloud`**; a de cluster é `lkc-`. É o
mesmo modelo do Curso 2 — cada recurso, sua credencial. Usar a errada dá erro de
autenticação que manda você procurar no lugar errado.

**3. Porque as métricas são amostradas a cada 60 segundos.** Raspar a cada 15s devolve
o mesmo valor quatro vezes e consome rate limit. Você paga em erro 429 sem ganhar
resolução.

**4. Quase sempre é má notícia.** Lag zero *estável* é saudável, mas zerar **de
repente** costuma significar: o consumidor morreu (não há quem meça o lag), o offset
foi resetado para o fim (`--to-latest`, pulando mensagens), ou o tópico parou de
receber. Alerta bom monitora lag alto **e** ausência de dado.

</details>

---

## Se algo não funcionou

**`401 Unauthorized`**
Key de escopo errado. Precisa ser `--resource cloud`.

**A resposta vem `{"data":[]}`**
Não houve atividade na janela consultada. Amplie o intervalo ou produza algo.

**Erro 429**
Rate limit. Espere um minuto e reduza a frequência.

**O `/export` vem vazio**
Confirme o `resource.kafka.id` na query string e que o cluster tem tópicos com dados.

---

## Glossário

| Termo | Definição |
|---|---|
| **Metrics API** | API de telemetria; a fonte por trás de toda a UI. |
| **Cloud API key** | Credencial de escopo organizacional — a terceira do bootcamp. |
| **`consumer_lag_offsets`** | A métrica rainha: distância entre produção e consumo. |
| **`retained_bytes`** | Bytes retidos por tópico — vira custo. |
| **`/export`** | Endpoint em formato Prometheus, sem agente. |
| **`for:` (alerta)** | Tempo que a condição deve persistir antes de disparar. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Storage virando fatura | [Aula 4](../aula4/README.md) |
| Quem fez o quê (audit) | [Aula 3](../aula3/README.md) |
| Lag do cluster link como RPO | [Aula 5](../aula5/README.md) |

**Fontes**

- Confluent — [Metrics API](https://docs.confluent.io/cloud/current/monitoring/metrics-api.html)
- Confluent — [API reference](https://api.telemetry.confluent.cloud/docs)
- Google SRE — [Monitoring distributed systems](https://sre.google/sre-book/monitoring-distributed-systems/)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 1 · Operação profissional](../aula1/README.md) | [Curso 7](../README.md) | [Aula 3 · Segurança](../aula3/README.md) |
