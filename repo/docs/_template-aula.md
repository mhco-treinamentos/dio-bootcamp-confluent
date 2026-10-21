<!--
TEMPLATE de guia de aula. Copie para cursoN/aulaM/README.md e preencha.

Placeholders: {{CURSO}} {{AULA}} {{TEMA}} {{DURACAO}} {{MAKE_PREFIX}} {{TOPIC}}

Blocos marcados com "REMOVER se a aula não tiver demo" só valem para aulas
práticas (25 das 34 aulas). Aulas conceituais (9) removem esses blocos.

Tom: direto, segunda pessoa, sem infantilização. Referência: demos/curso1/README.md
e PYCHARM.md. Prosa pt-BR, código/identificadores em inglês.

Estrutura de passo derivada de .claude/rules/labs.md (Goal / Comandos /
Output esperado / Validação / Cleanup) adaptada ao formato de aula.
-->

# Curso {{CURSO}} · Aula {{AULA}} — {{TEMA}}

> **Guia do aluno** · Bloco N — {{NOME DO BLOCO}}
> ⏱ ~{{DURACAO}} min · 📄 Exercício em papel — **sem Docker, sem terminal**
> <!-- OU, para aulas com demo: 💻 Prática no sandbox local · precisa de N terminais -->

{{Uma ou duas linhas dizendo o que esta aula constrói e por que ela importa para as próximas.}}

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter assistido à aula | ~{{DURACAO_VIDEO}} min |
| {{Papel e caneta / o sandbox no ar}} | {{detalhe}} |

<!-- REMOVER se a aula não tiver demo -->
```bash
make up          # sobe o sandbox (se ainda não estiver rodando)
make status      # confirma: containers Up e o broker respondendo
```

> ⚠️ O sandbox tem **um broker só**: RF=1 e sem ISR. As garantias de replicação
> do bootcamp (RF=3, `min.insync.replicas=2`) são conceito aqui e realidade no
> Confluent Cloud, a partir do Curso 2.
<!-- FIM do bloco de demo -->

---

## O que você vai saber fazer ao final

- {{Capacidade verificável, não tópico. Ex.: "Classificar uma mensagem como evento ou comando e justificar"}}
- {{...}}
- {{...}}

---

## Os conceitos em 90 segundos

{{Parágrafo curto — só o necessário para fazer o exercício.}}

{{Tabela ou diagrama de consulta — o aluno olha para isto enquanto resolve.}}

> 📖 As analogias e a explicação completa estão **na aula gravada**. Este guia
> assume que você já assistiu.

---

## Passo a passo

### Passo 1 — {{Título}} *(~{{N}} min)*

**Objetivo**: {{uma frase}}

**O que fazer**: {{instrução}}

<!-- REMOVER se a aula não tiver demo -->
**Terminais**: {{N}}

<!--
PREFIRA O COMANDO NATIVO ao alvo `make`. O aluno aprende vendo `kafka-topics
--create`, `kafka-console-consumer --group ...` — o `make` esconde justamente o
que a aula quer ensinar. Deixe a versão automatizada numa seção "Bônus" no fim,
para quem quiser repetir rápido.

Exceções em que o `make` é aceitável no corpo da aula: subir/derrubar o ambiente
(`make up` / `make down`) e demos que exigem código Python de verdade (a lógica
de dedup da Aula 5, por exemplo).
-->

```bash
docker exec bootcamp-kafka kafka-{{comando}} \
  --{{flag}} {{valor}} \
  --bootstrap-server localhost:9092
```

> ▶ **No PyCharm**: `{{Aula N · nome da configuration}}` — execuções simultâneas
> abrem em abas separadas.

**Output esperado** *(trecho real, capturado rodando — não invente)*
```
{{2-3 linhas de output verdadeiro}}
```

**O que observar**: {{o comportamento, não o pixel}}

**Validação**: {{comando independente que prova o resultado}}
<!-- FIM do bloco de demo -->

**Entregável**: {{o que deve existir ao terminar o passo}}

**Como saber que acertou**: {{critério objetivo, ou "gabarito abaixo"}}

---

### Passo 2 — {{...}}

{{Mesma estrutura.}}

---

## Gabarito

<details>
<summary><b>Passo 1 — {{título}} (clique para abrir)</b></summary>

{{Resposta + o porquê de cada item. Mesma numeração e ordem do exercício —
edite exercício e gabarito juntos, sempre.}}

</details>

<details>
<summary><b>Passo 2 — {{título}}</b></summary>

{{...}}

</details>

---

## Erros comuns

| Se você… | O que está confundindo |
|---|---|
| {{sintoma observável no exercício}} | {{o conceito por trás}} |

<!-- REMOVER se a aula não tiver demo -->
### Se algo não funcionou

**{{Sintoma}}**
```bash
{{comando de diagnóstico}}
```
{{Causa mais provável primeiro, com o comando de correção.}}
<!-- FIM do bloco de demo -->

---

<!-- REMOVER se a aula não tiver demo -->
## Limpar

```bash
make {{MAKE_PREFIX}}-teardown    # remove os topics desta aula
```

Vai parar por aqui? `make down` derruba o sandbox inteiro.

---
<!-- FIM do bloco de demo -->

## Glossário

| Termo | Definição |
|---|---|
| {{só os termos que ESTE exercício usa}} | {{...}} |

O glossário completo da aula está no material da aula gravada.
<!-- Se um docs/glossario.md acumulativo for criado, troque a linha acima por:
     Glossário completo do bootcamp: [glossario.md](../../glossario.md) -->


---

## Para ir além

**Onde isso reaparece**

| Conceito desta aula | Volta em |
|---|---|
| {{conceito}} | {{Aula/Curso}} |

**Fontes**

- {{Livros e links do material da aula}}

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| {{[Aula N-1](../aulaN-1/README.md) ou —}} | [Curso {{CURSO}}](../README.md) | {{[Aula N+1](../aulaN+1/README.md) ou "Aula N+1 · Tema (em breve)"}} |

<!--
Ao criar a próxima aula: volte aqui e transforme "(em breve)" em link.
-->
