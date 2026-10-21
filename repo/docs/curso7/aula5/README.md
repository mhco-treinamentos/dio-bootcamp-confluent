# Curso 7 · Aula 5 — O pior dia: DR, RPO/RTO e as garantias fim a fim

> **Guia do aluno** · Bloco 3 — Operação
> ⏱ ~45 min · 📄 **Exercício em papel** + 2 comandos de leitura
> **A última aula do bootcamp gravado.**

Trinta e duas aulas atrás, um evento era um JSON com verbo no passado. Hoje ele
atravessa CDC, contrato, Flink e chega ao operador. Falta uma pergunta: **e no dia em
que a região cai?**

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| O bootcamp inteiro | Esta aula consolida |
| Papel e caneta | O exercício principal |
| Terminal (opcional) | 2 comandos de leitura de preço |

> ⚠️ **Por que esta aula não tem hands-on de DR**: multi-region exige **dois clusters
> Dedicated**. Você vai calcular o preço no Passo 2 — e vai entender por que o
> bootcamp para no diagrama.

---

## O que você vai saber fazer ao final

- Situar seu cluster na **escada da disponibilidade** — e saber qual degrau é seu
- Declarar **RPO e RTO** com números, não com adjetivos
- Escrever um **runbook de failover** com dono e critério
- **Auditar** a garantia de entrega do seu pipeline, trecho a trecho

---

## Os conceitos em 90 segundos

Cada degrau responde a uma falha maior:

```
① BROKER caiu?  → RÉPLICAS (RF=3, ISR, acks=all)     ← Curso 1; automático
② ZONA caiu?    → MULTI-ZONE (réplicas entre AZs)     ← default do Cloud; o SLA
③ REGIÃO caiu?  → MULTI-REGION + Cluster Linking      ← SEU projeto, sua fatura
```

Os degraus ① e ② a plataforma já entrega. O ③ é decisão, custa e exige runbook.

E as duas métricas que todo plano de DR crava **antes** do desenho:

```
                    o DESASTRE
────────────────────────█────────────────────────►
         ◄── RPO ──►    █    ◄──── RTO ────►
   "quanto DADO posso   █   "quanto TEMPO posso
    perder?"            █    ficar fora?"
```

> 📖 A anatomia do Cluster Linking em modo contingência e a narrativa do game day estão
> **na aula gravada**.

---

## Passo 1 — Em que degrau você está? *(~8 min)*

**🖱 Pela Console**: cluster → **Cluster settings** — tipo (Basic) e availability (single-zone) na tela.

**⌨️ Pela CLI**:

```bash
confluent kafka cluster list
```

**Output esperado**
```
  Current |     ID      |      Name      | Type  | Cloud |  Region   | Availability | Status
----------+-------------+----------------+-------+-------+-----------+--------------+--------
  *       | lkc-9kwm1py | bootcamp-basic | BASIC | aws   | us-east-1 | single-zone  | UP
```

**O que observar**: `single-zone`. O cluster do bootcamp está no degrau **①** — réplicas
dentro de **uma** zona.

```bash
confluent kafka topic describe pg-payments.payments.transactions
```

**Output esperado**
```
+--------------------+-----------------------------------+
| Replication Factor | 3                                 |
| Partition Count    | 1                                 |
+--------------------+-----------------------------------+
```

`RF=3` — três cópias. Mas **as três estão na mesma zona**. Se a zona cair, as três caem
juntas.

Preencha no papel, para o seu cluster:

| Falha | Sobrevivo? | Por quê |
|---|---|---|
| Um broker morre | | |
| A zona `us-east-1a` cai | | |
| A região `us-east-1` cai | | |

<details>
<summary><b>Gabarito</b></summary>

| Falha | Sobrevivo? | Por quê |
|---|---|---|
| Um broker morre | **Sim** | RF=3 + ISR: outra réplica assume liderança |
| A zona cai | **Não** — em `single-zone` | As três réplicas vivem na mesma zona |
| A região cai | **Não** | Só existe um cluster, numa região |

**A distinção que custa caro no pior dia**: `RF=3` protege contra **máquina**, não
contra **zona**. Só `multi-zone` distribui as réplicas entre AZs — e é daí que vem o
99,99% do SLA que a Confluent anuncia.

**E o SLA não é o seu DR.** O 99,99% cobre a infraestrutura da plataforma dentro de uma
região. Perda de região é outro degrau — e é seu.

</details>

---

## Passo 2 — Quanto custa o degrau ③ *(~7 min)*

DR multi-region é **dois clusters + um link**. Os preços vêm da CLI (Aula 4):

```bash
confluent billing price list --cloud aws --region us-east-1 \
  | grep "Dedicated" | grep "Multi zone" | grep "Internet" \
  | grep -E "CKU price|per-link"
```

**Output esperado**
```
  CKU price                | Dedicated | Multi zone | Internet | $3.806 USD/CKU-hour
  Cluster linking per-link | Dedicated | Multi zone | Internet | $0.25 USD/Link-hour
```

A conta mínima:

```bash
python3 -c "
cku, link, h = 3.806, 0.25, 730
print(f'cluster primario (1 CKU) : \${cku*h:9.2f}/mes')
print(f'cluster DR       (1 CKU) : \${cku*h:9.2f}/mes')
print(f'cluster link             : \${link*h:9.2f}/mes')
print(f'{\"-\"*40}')
print(f'MINIMO                   : \${(cku*2+link)*h:9.2f}/mes')"
```

**Output esperado**
```
cluster primario (1 CKU) : $  2778.38/mes
cluster DR       (1 CKU) : $  2778.38/mes
cluster link             : $   182.50/mes
----------------------------------------
MINIMO                   : $  5739.26/mes
```

**O que observar**: ~**$5.739/mês**, e este é o **piso** — sem contar o tráfego
replicado ($0,25/GB de leitura no link) nem o storage duplicado.

> 💡 **A pergunta que dimensiona o investimento**: *quanto custa uma hora fora do ar?*
> Se uma hora de indisponibilidade custa $50.000 ao negócio, $5.739/mês é barato. Se
> custa $200, DR multi-region é desperdício elegante. **DR se justifica por requisito,
> não por moda.**

---

## Passo 3 — Declare RPO e RTO *(~10 min)*

Escolha um pipeline (o do bootcamp ou o do seu trabalho) e complete no papel:

```
O QUE PROTEJO ......... _______________________________
RPO (dado que aceito perder) .... _______________
RTO (tempo até voltar) .......... _______________
CUSTO DE 1 HORA FORA ............ _______________
```

Depois responda a pergunta difícil: **como você provaria esses dois números?**

<details>
<summary><b>Um exemplo completo</b></summary>

```
O QUE PROTEJO ... pipeline de autorização de transações de cartão
RPO ............. 30 segundos
RTO ............. 15 minutos
CUSTO/HORA ...... ~R$ 400.000 (volume médio × ticket × taxa)
```

**O que o RPO de 30 s exige do desenho:**
- Cluster Linking **contínuo** — o RPO real *é* o lag do link no instante do desastre
- **Monitorar esse lag** (Aula 2!) — se o link atrasa 5 minutos, seu RPO virou 5
  minutos e ninguém percebeu. **Um RPO não monitorado é uma esperança, não um objetivo.**
- Alerta quando o lag do link passar de 30 s

**O que o RTO de 15 min exige:**
- Detecção automática (alerta, não cliente ligando) — orçamento: 2 min
- Decisão humana com dono nomeado — 5 min
- `promote` dos mirror topics — 2 min
- Clientes reapontados — 5 min (config externalizada, **não** hardcoded)

**Como eu provaria**: **game day**. Simular a perda da região primária num ambiente de
teste, executar o runbook com cronômetro, e medir os dois números de verdade. Um plano
de DR nunca testado não é plano — é documento.

**A honestidade da aula**: RPO zero e RTO zero **não existem a custo finito**. DR maduro
é declarar números aceitáveis e desenhar para eles. Quem promete zero não fez a conta.

</details>

---

## Passo 4 — O runbook do pior dia *(~8 min)*

O desenho canônico, com as peças do Curso 2, Aula 5, em modo contingência:

```
REGIÃO PRIMÁRIA (ativa)              REGIÃO DR (standby)
cluster A ──── cluster link ───────► cluster B
topics ativos                        mirror topics (read-only,
producers + consumers                byte a byte, OFFSETS PRESERVADOS)
```

Escreva no papel os cinco passos do failover, e ao lado de cada um: **quem executa**.

<details>
<summary><b>O runbook, comentado</b></summary>

```
① DETECTAR   — o alerta dispara (Aula 2), não o cliente ligando
② DECIDIR    — humano nomeado aciona o plano
③ FAILOVER   — promover os mirror topics (viram graváveis)
④ REAPONTAR  — producers/consumers para o cluster B
⑤ MEDIR      — o RPO real = o que o link ainda não replicou
```

**Sobre ②** — a decisão é humana **de propósito**. Automatizar o failover regional
convida o failover acidental: uma partição de rede transitória vira uma migração
completa de região, e você acorda com clientes divididos entre dois clusters, ambos
achando que são o primário. Detecção automática, **decisão com dono**.

**Sobre ④** — o superpoder: **os offsets são preservados**. Os consumidores continuam
de onde estavam, sem reprocessar tudo nem pular mensagens. Sem isso, o failover viraria
o pesadelo de "de onde cada consumer recomeça?".

É a mesma sensação do rebalance do Curso 1, Aula 4 — em escala de região.

**O pré-requisito que quase todo mundo esquece**: o passo ④ só é rápido se o endereço
do cluster estiver em **configuração externalizada**. Bootstrap server hardcoded
transforma um failover de 5 minutos num deploy de emergência de 2 horas.

**E o failback** — o caminho de volta à região primária — é parte do plano, não
improviso. Ele costuma ser mais difícil que o failover, porque agora há dados novos no
cluster B que precisam voltar.

</details>

---

## Passo 5 — Audite as suas garantias *(~10 min)*

O arco que começou no Curso 1, Aula 5, fechado. Preencha a coluna vazia para o **seu**
pipeline:

| Trecho | Mecanismo | Garantia | Eu tenho? |
|---|---|---|---|
| Producer → broker | `acks=all` + idempotência + RF=3 | Escrita durável sem duplicata de retry | |
| Broker → consumer | Commit **após** processar | At-least-once | |
| Consumer → efeito | **Idempotência de negócio** | Duplicata inofensiva ≈ exactly-once | |
| CDC (banco → topic) | WAL + slot de replicação | Nada do banco se perde | |
| Sink (topic → destino) | Upsert por chave | Entrega sem duplicar | |
| Flink (topic → topic) | Checkpoint + transações | Exactly-once de processamento | |
| Entre regiões | Cluster link | DR com RPO mensurável | |

Depois responda: **em qual trecho uma duplicata ainda chegaria ao usuário final?**

<details>
<summary><b>Gabarito</b></summary>

**A resposta é o terceiro trecho: consumer → efeito.**

Os outros seis são **mecanismo** — a plataforma os entrega se você configurar. O
terceiro é **código seu**. E é o único que a plataforma não pode garantir por você.

```
Producer idempotente ......... configuração  ✅
Flink exactly-once ........... configuração  ✅
Sink com upsert .............. configuração  ✅
Seu consumidor deduplicando .. CÓDIGO SEU    ⚠️
```

**A garantia fim a fim é a do elo mais fraco.** De nada vale exactly-once no Flink se o
consumidor final envia um e-mail duas vezes, cobra duas vezes ou credita duas vezes.
Auditar o pipeline é auditar **cada linha da tabela**, não confiar na mais forte.

**A síntese que venceu na prática** (e é a recomendação do bootcamp):

> **at-least-once + idempotência onde o efeito acontece.**
> Barata, robusta, suficiente para a maioria. EOS transacional fica para onde a
> duplicata é inaceitável **e** o custo de latência cabe.

**E o alicerce de tudo**: o log imutável, replicado e replayável do Curso 1, Aula 3. A
estrutura mais simples que existe — um arquivo em que só se escreve no fim — carregando
o bootcamp inteiro nas costas.

</details>

---

## Passo 6 — Fixe os conceitos *(~5 min)*

1. `RF=3` protege contra perda de zona?
2. Por que a decisão de failover não deve ser automática?
3. Sua empresa quer "RPO zero". O que você responde?
4. O Flink garante exactly-once. Posso parar de deduplicar no consumidor final?

<details>
<summary><b>Gabarito</b></summary>

**1. Não, por si só.** `RF=3` significa três cópias — mas onde? Em `single-zone`, as
três ficam na mesma zona e caem juntas. Proteção de zona exige `multi-zone`, que
distribui as réplicas entre AZs. **RF é quantidade; availability é distribuição.**

**2. Porque o custo dos dois erros é assimétrico.** Um failover que demora 3 minutos a
mais custa 3 minutos. Um failover acidental por partição de rede transitória cria
**split-brain**: parte dos clientes escrevendo em A, parte em B, e uma reconciliação
manual dolorosa depois. Detecção automática (rápida e barata), decisão humana (lenta e
cara, mas só quando importa).

**3. Que RPO zero exige replicação síncrona entre regiões** — cada escrita esperando a
confirmação da outra região antes de retornar ao producer. Isso adiciona a latência de
rede inter-regional (dezenas a centenas de ms) a **toda** transação, sempre — não só no
dia do desastre.

A pergunta certa de volta: *"quanto custa 30 segundos de dados perdidos, comparado a
100 ms a mais em cada transação, todos os dias?"* Para quase todo negócio, a resposta é
que o RPO de segundos sai muito mais barato.

**4. Não.** Exactly-once no Flink tem **escopo**: o trecho Kafka → Flink → Kafka. Ele
garante que o processamento não duplica nem perde *dentro daquele trecho*.

O que ele **não** cobre: o que o seu consumidor faz depois de ler o resultado. Se ele
lê, chama uma API de pagamento e o commit falha antes de registrar, o reprocessamento
chama a API de novo. A garantia acabou na fronteira do Flink; a idempotência do efeito
é sua.

</details>

---

## Se algo não funcionou

**`confluent billing price list` não mostra Dedicated**
Confirme `--cloud aws --region us-east-1`. Nem todo par cloud/região tem todos os tiers.

**Meu cluster aparece como `multi-zone`**
Ótimo — você está no degrau ②. Ajuste as respostas do Passo 1.

**Quero testar Cluster Linking de verdade**
Exige dois clusters Dedicated (~$5.739/mês). Não faça isso com créditos de bootcamp.

---

## Glossário

| Termo | Definição |
|---|---|
| **Multi-zone** | Réplicas entre zonas de disponibilidade — a base do SLA de 99,99%. |
| **Multi-region DR** | Cluster de contingência em outra região, via Cluster Linking. |
| **RPO** | O máximo de dado que se aceita perder — ≈ o lag do link. |
| **RTO** | O máximo de tempo até a operação voltar. |
| **Mirror topic** | Cópia read-only alimentada pelo link, com offsets preservados. |
| **Failover / promote** | Promover os mirror topics e reapontar os clientes. |
| **Failback** | O caminho de volta ao primário — mais difícil que a ida. |
| **Split-brain** | Clientes divididos entre dois clusters, ambos se achando primários. |
| **Game day** | O teste real do plano: simular, executar, medir. |
| **Garantia fim a fim** | A semântica auditada trecho a trecho — a do elo mais fraco. |

---

## Você terminou o bootcamp gravado

Sete cursos. O que era um JSON com verbo no passado agora atravessa um pipeline que
você construiu, e que você sabe **observar**, **proteger**, **precificar** e
**recuperar**.

O que falta é seu: o **Desafio Final**. E o README dele pede exatamente as quatro
respostas deste curso:

| Pilar | O que o README cobra |
|---|---|
| ① Observabilidade | Métricas medidas, não estimadas |
| ② Segurança | O modelo de acesso, service account por camada |
| ③ Custos | Trade-offs justificados com números |
| ④ Confiabilidade | A semântica de entrega declarada |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| RF, ISR, `acks=all` | [Curso 1 · Aula 5](../../curso1/aula5/README.md) |
| Cluster Linking e offsets preservados | [Curso 2 · Aula 5](../../curso2/aula5/README.md) |
| Checkpoints e EOS do Flink | [Curso 4 · Aula 4](../../curso4/aula4/README.md) |
| Monitorar o lag do link como RPO | [Aula 2](../aula2/README.md) |

**Fontes**

- Confluent — [Cluster Linking: DR e Failover](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/dr-failover.html)
- Confluent — [SLA](https://www.confluent.io/confluent-cloud/sla/)
- Apache Kafka — [Message Delivery Semantics](https://kafka.apache.org/documentation/#semantics)
- Google SRE — [Managing Incidents](https://sre.google/sre-book/managing-incidents/)

---

| ← Anterior | ↑ Curso | Próximo → |
|---|---|---|
| [Aula 4 · Custos](../aula4/README.md) | [Curso 7](../README.md) | [Desafio Final](../../README.md) |
