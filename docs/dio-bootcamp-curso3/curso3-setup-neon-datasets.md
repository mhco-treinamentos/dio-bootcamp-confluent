# Curso 3 · Setup — Neon Postgres e os Datasets do Bootcamp

> **Bootcamp Confluent Cloud 2026 · Bloco 2 — apoio ao Curso 3 (Connect e CDC)**
> Duração alvo: ~8–10 min · Vídeo curto de setup, no estilo da Aula 0
> ⚠️ **Dataset em desenvolvimento** — tabelas e seed ilustrativos; atualizar este material quando a modelagem final fechar no projeto de demos

---

## 1. Objetivo

Deixar pronto o **banco de dados do bootcamp**: um Postgres gratuito no **Neon**, com replicação lógica habilitada (o pré-requisito do CDC), as tabelas do dataset criadas e populadas — a "empresa legada" que os Cursos 3 em diante vão integrar ao streaming.

---

## 2. Por que Neon *(conceito mínimo)*

- **Postgres de verdade, serverless e com free tier** — o aluno reproduz tudo sem cartão corporativo
- **Acessível pela internet** — o conector fully managed do Confluent Cloud alcança o banco sem VPN/túnel
- **Suporta replicação lógica** — a fundação do CDC log-based (o "porquê" é a Aula 2; aqui só habilitamos)
- Papel na narrativa: o Neon **representa o banco legado** da empresa fictícia do bootcamp — o sistema que "já existia antes do streaming"

---

## 3. Roteiro de Tela — Especificação

1. **Criar a conta e o projeto no Neon** — signup (free tier), projeto `bootcamp-dio`, região próxima da usada no Confluent Cloud (minimizar latência entre eles).
   - **Observação-alvo:** onde ficam **host, database, user e senha** — o aluno vai precisar deles no conector (Curso 3, Aula 2).

2. **Habilitar a replicação lógica** — nas configurações do projeto Neon (o equivalente gerenciado do `wal_level=logical`).
   - **Observação-alvo:** é um toggle de projeto — sem ele, o CDC da Aula 2 não conecta. Plantar: "o que isso liga por baixo é o assunto da Aula 2".

3. **Criar as tabelas do dataset** — executar o script DDL do projeto de demos no SQL Editor do Neon.
   - *(Placeholder — modelagem em desenvolvimento. Referência ilustrativa: `clientes`, `pedidos`, `itens_pedido` — o domínio de e-commerce que acompanha o bootcamp desde o Curso 1.)*
   - **Observação-alvo:** toda tabela com **primary key** — a Aula 2 mostra por que isso importa pro CDC (chave → partição → ordem, o Curso 1 de novo).

4. **Popular com o seed** — executar o script de carga inicial do projeto de demos; conferir com um `SELECT` de contagem.
   - **Observação-alvo:** esses dados existentes são o que o CDC vai capturar como **snapshot inicial** (Aula 2).

5. **Criar o usuário/role para o conector** — role dedicada com permissão de replicação (não usar o owner do banco).
   - **Observação-alvo:** o princípio da Aula 2 do Curso 2 (identidade de aplicação, escopo mínimo) vale para o banco também.

6. **Teste de conectividade** — validar host/porta/SSL com uma conexão externa simples.

**Fechamento** *(~30s)*: "banco legado de pé, replicação ligada, dataset carregado. No Curso 3, esse banco entra no streaming — sem escrevermos uma linha de código de integração."

---

## 4. Pontos de Atenção

- **Free tier tem limites e eles mudam** — o vídeo narra a estrutura; valores exatos (storage, compute hours, nº de branches) se confirmam na página atual do Neon.
- **Credenciais do banco = mesmo tratamento das API keys** — nunca no Git, nunca em chat; o conector as recebe via configuração segura.
- **Auto-suspend do Neon** (o compute dorme quando ocioso no free tier) — inofensivo para o bootcamp, mas explica uma eventual latência fria na primeira query/conexão do conector.
- **Dataset placeholder** — este vídeo deve ser gravado por último dentro do Curso 3, quando a modelagem final estiver fechada no projeto de demos.

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Neon** | Postgres serverless gerenciado com free tier — o "banco legado" do bootcamp. |
| **Replicação lógica (logical replication)** | Recurso do Postgres que expõe as mudanças do WAL em formato consumível — a porta do CDC. |
| **WAL (Write-Ahead Log)** | O log de transações do Postgres (dissecado na Aula 2 do Curso 3). |
| **Seed** | Carga inicial de dados de exemplo nas tabelas. |
| **Primary key** | Identidade da linha — vira a key do evento CDC (ordem por partição). |

---

## 6. Referências

**Documentação oficial:**

- Neon — *Get Started* — https://neon.tech/docs/get-started-with-neon/signing-up
- Neon — *Logical Replication* — https://neon.tech/docs/guides/logical-replication-guide
- Neon — *Connect Confluent/Kafka (CDC)* — https://neon.tech/docs/guides/logical-replication-kafka-confluent
- PostgreSQL — *Logical Replication* — https://www.postgresql.org/docs/current/logical-replication.html
