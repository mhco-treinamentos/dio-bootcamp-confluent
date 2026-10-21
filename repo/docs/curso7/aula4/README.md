# Curso 7 · Aula 4 — Custos: onde o seu dinheiro realmente foi

> **Guia do aluno** · Bloco 3 — Operação
> ⏱ ~45 min · ☁️ **Custo ≈ zero** (ler preços e mudar configs não cobra) · 1 terminal

Você vai descobrir que **81% do storage do seu cluster não é dado de negócio**. E vai
aprender a fazer a conta que decide um contrato — com os números reais da sua conta,
não com estimativas de slide.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Aula 2 deste curso | A Metrics API e a key de escopo `cloud` |
| Ter feito os Cursos 3 e 4 | Precisa haver histórico de gasto |
| `python3` | Para as contas |

---

## O que você vai saber fazer ao final

- Ler o **catálogo de preços** pela CLI, sem depender de página de marketing
- Descobrir o que está pesando no seu cluster — e se surpreender
- Calcular o efeito de `retention.ms` em reais, antes de mudar
- Escolher entre **retenção** e **compactação** com uma régua, não com intuição
- Fazer a conta de **sizing** do seu próprio projeto

---

## Os conceitos em 90 segundos

Cada recurso tem sua régua. Conhecer a régua **é** a otimização:

| Recurso | Cobra por | Ocioso custa? |
|---|---|---|
| Cluster **Basic** | uso (throughput, storage) | ~zero |
| Cluster **Dedicated** | CKU-hora **provisionada** | **Sim, tudo** |
| Conector | task-hora | **Sim** |
| Statement Flink | CFU-hora | **Sim** |
| Compute pool Flink ocioso | — | **Não** |
| Storage | GB-hora retido | Sim |

```
elástico cobra o que FLUI      →  Basic, Standard
provisionado cobra o que RESERVA →  Dedicated
```

E os três vilões do teardown, agora com explicação econômica: conector, statement e
Tableflow cobram **por existir**, não por fazer algo útil.

> 📖 A discussão de TCO (gerenciado × self-managed) e a arquitetura do Tiered Storage
> estão **na aula gravada**.

---

## Passo 1 — O catálogo de preços, pela CLI *(~7 min)*

Preço de marketing envelhece. Aqui você lê o preço que **a sua conta paga hoje**.

**🖱 Pela Console**: ☰ → **Billing & payment** — a aba **Costs** mostra o gasto por recurso/dia (o equivalente do `billing cost list`).

**⌨️ Pela CLI** — os preços unitários, direto do rate card:

```bash
confluent billing price list --cloud aws --region us-east-1 \
  | grep "CKU price" | grep -E "Basic|Standard|Enterprise|Dedicated" \
  | grep -vE "Peered|Private|Transit" | sort -u
```

**Output esperado**
```
  CKU price                | Basic        | Multi zone   | Internet        | $0.135 USD/CKU-hour
  CKU price                | Basic        | Single zone  | Internet        | $0.135 USD/CKU-hour
  CKU price                | Dedicated    | Multi zone   | Internet        | $3.806 USD/CKU-hour
  CKU price                | Dedicated    | Single zone  | Internet        | $3.806 USD/CKU-hour
  CKU price                | Enterprise   | Multi zone   |                 | $2.25 USD/CKU-hour
  CKU price                | Standard     | Multi zone   | Internet        | $0.75 USD/CKU-hour
  CKU price                | Standard     | Single zone  | Internet        | $0.75 USD/CKU-hour
```

**O que observar** — a escada de preço por CKU-hora, e o que ela significa por mês:

| Tipo | $/CKU-hora | 24/7 (730h) |
|---|---|---|
| Basic | $0,135 | ~$99 |
| Standard | $0,75 | ~$548 |
| Enterprise | $2,25 | ~$1.643 |
| **Dedicated** | **$3,806** | **~$2.778** |

> ⚠️ **A diferença que não está na tabela**: Basic e Standard cobram por **uso** — o
> CKU-hora é o teto, não o piso. Dedicated cobra a **capacidade reservada**, use ou
> não. É a diferença entre táxi e carro alugado.

Os preços do resto:

```bash
confluent billing price list --cloud aws --region us-east-1 -o json \
  | python3 -c "
import json,sys
for r in json.load(sys.stdin):
    if r.get('cluster_type')=='basic' and r['availability']=='high':
        print(f\"{r['metric']:25s} \${r['price']}\")"
```

**Output esperado**
```
ClusterLinkingPerLink     $0.25
ClusterLinkingRead        $0.25
ClusterLinkingWrite       $0.13
KSQLNumCSUs               $0.22222222
KafkaNetworkRead          $0.05
KafkaNetworkWrite         $0.05
KafkaNumCKUs              $0.135
KafkaRestProduce          $0.01
KafkaStorage              $0.00010959
```

**O que observar** em `KafkaStorage`: **$0,00010959 por GB-hora**. Parece nada. Faça a
conta anual:

```
$0,00010959 × 24 × 365 = $0,96 por GB-ano
```

Quase **um dólar por GB por ano**. Um tópico de 500 GB custa $480/ano só de storage —
e cada tópico derivado dele (enriquecido, classificado) custa de novo.

---

## Passo 2 — O que está pesando (a surpresa) *(~8 min)*

Use a key de escopo `cloud` da Aula 2:

Perdeu o secret da Aula 2? Crie outra: `confluent api-key create --resource cloud --description "Metrics API"`.

```bash
export MK="SUA-API-KEY"
export MS="cflt-SEU-SECRET-AQUI"
export CLUSTER=$(confluent kafka cluster list -o json | python3 -c "import json,sys;print(json.load(sys.stdin)[0]['id'])")
```

A mesma query de storage da Aula 2, Passo 6 — agora com o total e a proporção:

```bash
curl -s -u "$MK:$MS" -H "Content-Type: application/json" \
  https://api.telemetry.confluent.cloud/v2/metrics/cloud/query \
  -d @- <<EOF | python3 -c "
import json,sys
d=json.load(sys.stdin)['data']
tot={}
for p in d: tot[p['metric.topic']]=max(tot.get(p['metric.topic'],0),p['value'])
for t,v in sorted(tot.items(), key=lambda x:-x[1])[:8]:
    print(f'  {v/1024:9.1f} KB  {t}')
total=sum(tot.values())
lixo=sum(v for t,v in tot.items() if t.startswith(('dlq-','__debezium-heartbeat')))
print(f'\nTOTAL: {total/1048576:.2f} MB')
print(f'DLQ + heartbeat: {lixo/1048576:.2f} MB ({lixo/total:.0%})')"
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

**Output esperado** (do cluster de validação — o seu vai variar)
```
     7661.0 KB  dlq-lcc-9kwm6k7
     4643.7 KB  dlq-lcc-v71qx55
     1611.5 KB  pg-payments.payments.transactions
     1184.0 KB  flat-payments.payments.transactions
       79.7 KB  pg-payments.payments.accounts
       38.5 KB  lab.transactions.datagen
       29.6 KB  transacoes_grandes
        8.6 KB  __debezium-heartbeat-lcc-223xp3y.flat-payments

TOTAL: 14.95 MB
DLQ + heartbeat: 12.08 MB (81%)
```

**O que observar — e este é o achado da aula:**

**81% do storage não é dado de negócio.** São DLQs de conectores que já foram deletados
e tópicos de heartbeat do Debezium.

> 💡 **Não viu nenhum `dlq-lcc-` no seu cluster?** Ou você nunca configurou DLQ nos
> conectores do Curso 3, ou já limpou. O número que importa é a **proporção** entre
> subprodutos e dado de negócio — se a sua for melhor que 81%, ótimo. Siga a leitura;
> a lição vale igual.

```
dado de negócio ...... 2,87 MB  (19%)
DLQ + heartbeat ..... 12,08 MB  (81%)   ← subprodutos que ninguém pediu
```

Três lições dentro de um número:

1. **Os DLQs sobreviveram aos conectores.** `dlq-lcc-9kwm6k7` refere-se a um conector
   `lcc-` que não existe mais. Você deletou o conector; o tópico ficou — pagando.
2. **É o aterro sanitário previsto.** O Curso 3, Aula 4 avisou: *"DLT sem processo de
   reprocesso vira aterro"*. Aqui está, medido em bytes.
3. **Escala é o que assusta.** Em 15 MB isso custa frações de centavo. Num pipeline de
   produção com 500 GB de DLQ acumulado, são **~$480/ano** pagos para armazenar dados
   que ninguém vai ler.

Conte os seus:

```bash
confluent kafka topic list | grep -cE "dlq-lcc-|__debezium-heartbeat"
```

**Output esperado** (no cluster de validação, antes da limpeza)
```
24
```

**Vinte e quatro tópicos** que nenhum lab criou de propósito — 17 DLQs e 7 heartbeats,
todos de conectores que já não existem.

### E agora limpe

Medir sem agir é hobby. Confirme primeiro que **nenhum conector está ativo**:

```bash
confluent connect cluster list
```

```
None found.
```

> ⚠️ **Só continue se vier `None found.`** Um DLQ de conector ativo está em uso —
> deletá-lo faz o conector recriá-lo ou falhar.

```bash
# lista o que será apagado — leia ANTES de executar a próxima
confluent kafka topic list -o json | python3 -c "
import json,sys
alvos=[t['name'] for t in json.load(sys.stdin)
       if t['name'].startswith('dlq-lcc-')
       or t['name'].startswith('__debezium-heartbeat')]
print('\n'.join(alvos)); print(f'--- {len(alvos)} topicos', file=sys.stderr)"
```

Se a lista estiver correta, apague:

```bash
confluent kafka topic list -o json | python3 -c "
import json,sys
for t in json.load(sys.stdin):
    n=t['name']
    if n.startswith('dlq-lcc-') or n.startswith('__debezium-heartbeat'):
        print(n)" | while read t; do
  confluent kafka topic delete "$t" --force && echo "  ok $t"
done
```

**Validação:**
```bash
confluent kafka topic list | grep -cE "dlq-lcc-|__debezium-heartbeat"
```
```
0
```

**O resultado medido no cluster de validação:**

```
antes ..... 14,95 MB em 35 tópicos
depois .....2,89 MB em 11 tópicos     ← -81%
```

> 💡 **`dlq_transacoes` não entra nessa limpeza** — ele é o DLQ que *você* criou à mão
> no Curso 3, Aula 4, com nome próprio. O filtro `dlq-lcc-` pega só os
> auto-gerados pelos conectores gerenciados. **Nomear seus recursos é o que os separa
> do entulho automático** — uma razão prática para não aceitar nomes gerados.

---

## Passo 3 — A retenção, em dinheiro *(~8 min)*

Veja a config atual:

**🖱 Pela Console**: tópico → **Configuration** → **Edit settings** → `retention.ms` — a mesma alavanca, com o risco de parecer inofensiva sem a conta ao lado.

**⌨️ Pela CLI**:

```bash
confluent kafka topic configuration list pg-payments.payments.transactions \
  | grep -E "retention|cleanup"
```

**Output esperado**
```
  cleanup.policy                          | delete              | false
  delete.retention.ms                     | 86400000            | false
  retention.bytes                         | -1                  | false
  retention.ms                            | 604800000           | false
```

**O que observar**: `604800000` ms = **7 dias**. É o default do Kafka — e default não é
decisão. Ninguém escolheu 7 dias para este tópico; ele veio assim.

A conta, antes de mudar qualquer coisa:

```bash
python3 -c "
PRECO = 0.00010959   # USD por GB-hora (Basic, do Passo 1)
mbs = 5              # MB/s de entrada — o SEU número, da Metrics API
rf  = 3              # replication factor

for dias in (3, 7, 30, 90):
    gb = mbs * 86400 * dias / 1024 * rf
    print(f'{dias:3d} dias -> {gb:9.1f} GB  =  \${gb*PRECO*730:8.2f}/mes')"
```

**Output esperado**
```
  3 dias ->    3796.9 GB  =  $  303.75/mes
  7 dias ->    8859.4 GB  =  $  708.76/mes
 30 dias ->   37968.8 GB  =  $ 3037.53/mes
 90 dias ->  113906.2 GB  =  $ 9112.58/mes
```

**O que observar**: de 7 para 30 dias, a fatura **quadruplica**. E o cálculo já inclui
o `rf=3` — cada byte é armazenado três vezes.

> 💡 **O multiplicador escondido**: este cálculo é de **um** tópico. O seu pipeline tem
> o bruto (CDC), o achatado (SMT), o enriquecido (Flink) e o classificado. Quatro
> tópicos com o mesmo dado em formas diferentes. A retenção multiplica em todos.

Mudar é uma linha — para 3 dias:

```bash
confluent kafka topic update pg-payments.payments.transactions --config retention.ms=259200000
```

**Output esperado**
```
Updated the following configuration values for topic "pg-payments.payments.transactions":
      Name     |   Value   | Read-Only
---------------+-----------+------------
  retention.ms | 259200000 | false
```

Restaure ao terminar:
```bash
confluent kafka topic update pg-payments.payments.transactions \
  --config retention.ms=604800000
```

> ⚠️ **Reduzir retenção apaga dado imediatamente e não tem volta.** Em produção, a
> pergunta é: *quanto replay eu realmente preciso?* Auditoria exige 90 dias? São 90
> dias, com justificativa. "Vai que" não é requisito — é $9.112/mês.

---

## Passo 4 — Compactação: a régua *(~7 min)*

```bash
confluent kafka topic create demo-estado-contas \
  --partitions 1 --config cleanup.policy=compact

confluent kafka topic configuration list demo-estado-contas \
  | grep -E "cleanup.policy|min.cleanable|min.compaction"
```

**Output esperado**
```
Created topic "demo-estado-contas".
  cleanup.policy                          | compact             | false
  min.cleanable.dirty.ratio               | 0.5                 | true
  min.compaction.lag.ms                   | 0                   | false
```

**O que observar**: nenhum `retention.ms` governando o tamanho. Com `compact`, o tópico
guarda **o último valor por chave** — o tamanho para de crescer com o **tempo** e passa
a crescer com a **cardinalidade** (quantas chaves distintas existem).

```
delete  → o FILME    (toda mudança, por N dias)      → cresce com o tempo
compact → a FOTO     (o valor atual de cada chave)   → cresce com as chaves
```

A régua que decide:

| O tópico guarda | Política | Por quê |
|---|---|---|
| **Fatos** (transações, cliques, logs) | `delete` + retenção pensada | Cada evento é uma verdade histórica |
| **Estado** (saldo, cadastro, último status) | `compact` | Só o valor atual importa |
| Estado **com** janela de história | `compact,delete` | O híbrido |

> ⚠️ **Compaction em tópico de fatos destrói a história.** Se duas transações do mesmo
> cartão compartilham a chave, a segunda apaga a primeira — e o replay do Curso 4 passa
> a mentir. **Régua antes de alavanca.**

No seu pipeline: `pg-payments.payments.accounts` é **estado** (o cadastro atual da
conta) — candidato natural a compaction. `pg-payments.payments.transactions` é
**fatos** — jamais.

Limpe:
```bash
confluent kafka topic delete demo-estado-contas --force
```

---

## Passo 5 — A conta de sizing do seu projeto *(~9 min)*

O esqueleto que dimensiona um cluster, preenchido com **os seus** números:

```
① VOLUME     msgs/s × tamanho médio = MB/s in;  × fan-out = MB/s out
② PARTIÇÕES  paralelismo alvo × nº de tópicos
③ CONEXÕES   apps × instâncias × clients
④ RETENÇÃO   MB/s in × segundos × RF = storage em regime
```

Meça ① com a Metrics API — não estime:

```bash
curl -s -u "$MK:$MS" -H "Content-Type: application/json" \
  https://api.telemetry.confluent.cloud/v2/metrics/cloud/query \
  -d @- <<EOF | python3 -c "
import json,sys
d=json.load(sys.stdin)['data']
if not d: print('sem trafego na janela'); raise SystemExit
pico=max(x['value'] for x in d)
med=sum(x['value'] for x in d)/len(d)
print(f'media: {med/3600/1024:8.2f} KB/s')
print(f'PICO:  {pico/3600/1024:8.2f} KB/s   <- dimensione por ESTE')
print(f'ratio: {pico/med:.1f}x')"
{
  "aggregations": [{"metric": "io.confluent.kafka.server/received_bytes"}],
  "filter": {"field": "resource.kafka.id", "op": "EQ", "value": "$CLUSTER"},
  "granularity": "PT1H",
  "intervals": ["PT72H/now"]
}
EOF
```

**Output esperado** (o seu vai variar conforme o que rodou)
```
media:     0.16 KB/s
PICO:      0.53 KB/s   <- dimensione por ESTE
ratio: 3.3x
```

**O que observar**: o pico é **~3× a média**, num lab de estudo com tráfego mínimo. Em
produção essa razão costuma ser bem maior — e o cluster que atende a média morre no
primeiro pico real.

> 💡 **Picos dimensionam, médias enganam.** Cluster elástico absorve picos (pagando por
> eles); provisionado exige tê-los no papel **antes** de assinar o contrato.

Agora a decisão, com os preços do Passo 1:

| Se o seu pico é | Cluster | Por quê |
|---|---|---|
| < 100 MB/s, sem requisito de rede | **Basic/Standard** | Paga o que flui |
| Alto e **previsível**, 24/7 | **Dedicated** | Reservar sai mais barato que picar |
| Requisito de rede privada | **Enterprise** | Private Link |
| Altíssimo, tolera latência | **Freight** | Custo de rede mínimo |

**A regra de decisão**: só faz sentido pagar Dedicated ($2.778/mês) quando o consumo
elástico equivalente passaria disso — e quando você precisa da **previsibilidade** da
fatura, não só do preço.

---

## Passo 6 — Fixe os conceitos *(~6 min)*

1. Um tópico recebe 5 MB/s e tem 4 consumidores. O fan-out afeta storage ou throughput?
2. Por que compute pool Flink ocioso custa zero, mas conector parado custa?
3. Telemetria IoT: 800 MB/s, tolera segundos de latência, replay de 3 dias. Qual
   cluster e quais alavancas?
4. Seu DLQ tem 500 GB. Quanto custa por ano — e qual a pergunta certa?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. Só throughput out.** O storage é definido por entrada × retenção × RF — quantas
vezes você **lê** não muda quanto está guardado. Mas o egress multiplica: 5 MB/s × 4 =
20 MB/s de saída, a $0,05/GB. São ~**$2.531/mês** só de leitura — mais que o storage de
7 dias do mesmo tópico. **Fan-out é a dimensão que mais gente esquece de dimensionar.**

**2. Porque a régua é diferente.** O pool Flink cobra por **CFU consumida** — sem
statement rodando, o consumo é zero. O conector cobra por **task-hora**: a task existe e
está alocada, esteja ela processando ou parada. Um cobra o trabalho; o outro, a
alocação. É a mesma diferença de elástico × provisionado, em escala menor.

**3. Freight** é o primeiro a investigar — foi desenhado exatamente para esse perfil
(volume extremo, latência tolerante), trocando latência de ponta por custo de
rede/replicação drasticamente menor. As alavancas:
- **Retenção curta** — 3 dias é o requisito, então 3 dias. A 800 MB/s, cada dia extra
  são ~207 TB (já com RF=3).
- **Tiered storage** — o histórico vai para object storage, muito mais barato que disco
  de broker.

E o que **não** entra: compaction. Telemetria é fato, não estado.

**4. ~$480/ano** (500 × $0,00010959 × 730 × 12 ≈ $480).

Mas a pergunta certa não é *"como reduzo esse custo?"* — é **"por que há 500 GB de
mensagens rejeitadas que ninguém processou?"** O custo é o sintoma; o problema é que o
pipeline está descartando dados em silêncio há muito tempo. Apagar o DLQ resolve a
fatura e **esconde a falha**. Reprocessar resolve as duas coisas.

</details>

---

## Se algo não funcionou

**`confluent billing price list` vem vazio**
Confirme `--cloud` e `--region` (ex.: `aws` / `us-east-1`).

**`confluent billing cost list` não mostra os dias recentes**
Normal — o billing consolida com atraso de 1 a 2 dias. Para o custo de hoje, use os
preços do catálogo × os números da Metrics API.

**A query da Metrics API retorna `{"data":[]}`**
Sem tráfego na janela. Amplie o intervalo.

**Reduzi a retenção e perdi dados**
Sem volta. Em produção, valide o requisito de replay antes de reduzir.

---

## Glossário

| Termo | Definição |
|---|---|
| **eCKU / CKU** | Capacidade elástica (por uso) × provisionada (reservada por hora). |
| **GB-hora** | A unidade de storage — $0,00010959 no Basic ≈ $0,96/GB-ano. |
| **CFU-hora** | A unidade do Flink — $0,21, só com statement rodando. |
| **Fan-out** | Quantas vezes cada byte produzido é lido — multiplica o egress. |
| **`retention.ms`** | Quanto tempo o dado fica — a alavanca de custo mais barata. |
| **`cleanup.policy`** | `delete` (filme) × `compact` (foto). |
| **Tiered Storage** | Camada quente (disco) + fria (object storage). |
| **Freight** | Cluster type para volume extremo tolerante a latência. |
| **Storage em regime** | Onde o storage estabiliza: entrada × retenção × RF. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| `retention.ms` e `cleanup.policy` | [Curso 1 · Aula 3](../../curso1/aula3/README.md) |
| Escolha do cluster type | [Curso 2 · Aula 1](../../curso2/aula1/README.md) |
| A Metrics API que alimenta o sizing | [Aula 2](../aula2/README.md) |
| Custo de um segundo cluster para DR | [Aula 5](../aula5/README.md) |

**Fontes**

- Confluent — [Billing](https://docs.confluent.io/cloud/current/billing/overview.html)
- Confluent — [Pricing](https://www.confluent.io/confluent-cloud/pricing/)
- Confluent — [Cluster Types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)
- Apache Kafka — [KIP-405: Tiered Storage](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 3 · Segurança](../aula3/README.md) | [Curso 7](../README.md) | [Aula 5 · DR e garantias](../aula5/README.md) |
