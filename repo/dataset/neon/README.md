# Dataset Neon — o "banco legado" do bootcamp

Postgres serverless no [Neon](https://neon.tech) (free tier) representando o sistema legado da empresa fictícia do bootcamp — a fonte que o Curso 3 integra ao streaming via CDC (Debezium fully managed), **sem tocar na aplicação**.

Domínio: **payments** (dados 100% sintéticos — CPFs/CNPJs com check digit válido, PAN sempre mascarado, nunca CVV/PIN).

## Arquivos (rodar nesta ordem)

| Arquivo | O que faz | Onde rodar |
|---|---|---|
| `01_ddl.sql` | Cria o schema `payments` e as 5 tabelas (todas com PK) | SQL Editor do Neon ou psql |
| `02_seed.sql` | Carga inicial determinística (**gerado** — não editar na mão) | SQL Editor do Neon ou psql |
| `03_cdc_setup.sql` | Role `cdc_user` (least privilege), `REPLICA IDENTITY FULL`, publication | psql (precisa de role com privilégio) |
| `99_teardown.sql` | ⚠️ Destrói tudo (slot → publication → schema → role) | psql |

```bash
# Via psql (variáveis do .env — ver repo/.env.example)
export PGURL="postgresql://$PG_USER:$PG_PASSWORD@$PG_HOST/$PG_DATABASE?sslmode=require"
psql "$PGURL" -f 01_ddl.sql
psql "$PGURL" -f 02_seed.sql
psql "$PGURL" -f 03_cdc_setup.sql
```

## Pré-requisito Neon (antes do 03)

Habilitar **Logical Replication** nas configurações do projeto Neon (equivalente gerenciado do `wal_level=logical`). Sem isso o conector CDC da Aula 2 não conecta. Confirme com `SHOW wal_level;` → `logical`.

## Volumes do seed (seed=42, âncora temporal fixa 2026-07-01)

| Tabela | Registros | Nota |
|---|---|---|
| `customers` | 200 | CPF válido, endereço BR |
| `accounts` | 240 | todo customer tem ≥1 conta |
| `cards` | 320 | PAN mascarado `BIN6******last4`; UF de emissão = UF do titular |
| `merchants` | 60 | MCC ISO 18245 (top-10 ≈ 70% do volume) |
| `transactions` | 2 000 | log-normal ~R$ 80; Pareto por conta; ~1% fraude injetada (`fraud_flag`) |

Regenerar (mesmo output, byte a byte):

```bash
cd repo && python -m dataset.neon.generate_seed
```

## Verificação

```sql
SELECT (SELECT count(*) FROM payments.customers)    AS customers,
       (SELECT count(*) FROM payments.accounts)     AS accounts,
       (SELECT count(*) FROM payments.cards)        AS cards,
       (SELECT count(*) FROM payments.merchants)    AS merchants,
       (SELECT count(*) FROM payments.transactions) AS transactions;
--  200 | 240 | 320 | 60 | 2000
```

## Cuidados

- **Credenciais do Neon = mesmo tratamento das API keys**: `.env` local, nunca no Git.
- **Auto-suspend do free tier**: a primeira conexão após ociosidade tem latência fria — inofensivo, mas explica um conector demorando ~10s para conectar.
- **Conector CDC parado sem deletar o slot** = WAL acumulando no Neon. Teardown do lab sempre deleta o conector **e** confere `pg_replication_slots`.
- As tabelas capturadas pela publication são `accounts`, `cards`, `transactions` (as do lab do módulo 3). `customers`/`merchants` entram depois via `ALTER PUBLICATION ... ADD TABLE`.
- `fraud_flag`/`fraud_rule` são **ground truth sintético** para validar os labs de CEP (módulo 4) e ML (módulo 6) — num sistema real esses campos não existiriam na tabela transacional.
