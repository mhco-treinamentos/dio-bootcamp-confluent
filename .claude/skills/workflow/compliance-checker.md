# Compliance Checker

## Skill: `/compliance-checker`

Valida conformidade **LGPD** e **PCI-DSS** (domínio pagamentos — PIX/cartão) na plataforma do bootcamp no Confluent Cloud, usando **Stream Governance** para descoberta e inventário. Base da **Live 3** (governança: lineage, anonimização on-the-fly, ACLs por tópico, retenção como compliance).

## Usage

```
/compliance-checker <scope> [options]
```

### Scopes
- `lgpd` — Lei Geral de Proteção de Dados
- `pci` — PCI-DSS (dados de cartão)
- `access` — RBAC/ACLs/service accounts
- `full` — Todos

---

## LGPD

### Classificação de dados (domínio payments)

| Categoria | Campos | Tratamento |
|-----------|--------|------------|
| Identificação pessoal | CPF, CNPJ, nome | Tag `["PII","LGPD"]`, tokenizar p/ analytics |
| Contato | email, telefone, CEP | Tag `["PII","LGPD"]`, mascarar fora do lab de origem |
| Financeiro sensível | saldo, limite, histórico transacional | Acesso por ACL, retenção mínima |
| Cartão (PCI) | PAN, CVV | **PAN sempre mascarado (BIN+last4); CVV nunca em topic** |

### Checklist PII

```markdown
Kafka Topics:
- [ ] Todo campo PII com `confluent:tags: ["PII", "LGPD"]` no Avro (hook bloqueia sem)
- [ ] Campos de cartão com `confluent:tags: ["PCI"]`
- [ ] PII tokenizada (SHA-256 via Flink SQL) antes dos consumers analíticos
- [ ] Nenhum PII em nome de topic ou message key (usar surrogate keys)
- [ ] Dados 100% sintéticos — NUNCA dados de produção em lab

Stream Governance (Live 3):
- [ ] Stream Catalog com tags PII/PCI propagadas para topics derivados
- [ ] Stream Lineage cobrindo Postgres → Kafka → Flink → Tableflow
- [ ] Anonimização on-the-fly com Flink SQL para consumo analítico
- [ ] Retenção mínima como princípio (retention.ms curto = compliance)

Criptografia:
- [ ] SASL_SSL em todas as conexões (TLS 1.2+) — default do Confluent Cloud
- [ ] At-rest encryption (default nos clusters cloud)

Acesso:
- [ ] Service account por lab, least privilege (ACLs por prefixo)
- [ ] Topics com PII restritos aos consumers autorizados
- [ ] API keys amarradas a service accounts (nunca key de admin em producer)
```

### Direitos do titular (como demonstrar em streaming)

| Direito | Implementação no bootcamp |
|---------|---------------------------|
| Acesso | Consulta por token de CPF via Flink SQL |
| Retificação | Evento de correção propagado via CDC |
| Eliminação | Tombstone em topic compactado + retenção curta nos demais |
| Portabilidade | Export estruturado via Flink SQL / tabela Iceberg |

---

## PCI-DSS (cartão)

```markdown
- [ ] PAN nunca em claro em nenhum topic (SMT MaskField na borda do conector)
- [ ] `card_pan_masked` = BIN + last4 (`XXXXXXXX******XXXX`)
- [ ] CVV nunca persistido (nem no Postgres fonte)
- [ ] Topics com dados de cartão: ACL read apenas para consumers de fraude
- [ ] Tokenização irreversível (SHA-256) para analytics; pseudonimização quando
      reversibilidade for necessária (ver rules.md)
```

---

## Acesso (RBAC/ACLs)

```bash
# Inventário de service accounts e ACLs
confluent iam service-account list -o json | jq -r '.[].id' | while read sa; do
  echo "=== $sa ==="
  confluent kafka acl list --service-account "$sa"
done

# API keys por service account (idade/rotação)
confluent api-key list -o json | jq -r '.[] | "\(.key) \(.owner) \(.created)"'
```

```markdown
- [ ] sa por lab: sa-lab-modulo-0X, descrição preenchida
- [ ] ACLs por prefixo (dev.payments. / lab.payments.), nunca wildcard total
- [ ] Nenhuma API key órfã (sem service account)
- [ ] Keys de labs encerrados deletadas no teardown
```

---

## Comandos de validação

### PII/PCI tags no Schema Registry

```bash
# Subjects e schemas com campos taggeados
for subject in $(confluent schema-registry subject list -o json | jq -r '.[]'); do
  schema=$(confluent schema-registry schema describe --subject "$subject" --version latest 2>/dev/null)
  hits=$(echo "$schema" | grep -cE '"PII"|"PCI"|"LGPD"' || true)
  [ "$hits" -gt 0 ] && echo "SENSITIVE: $subject ($hits tag refs)"
done
```

### Varredura local (antes do registro — hook também cobre)

```bash
# .avsc com cpf/email/pan sem confluent:tags
grep -rlE '"name":\s*"(cpf|cnpj|email|phone|card_pan)' repo/shared/schemas/ \
  | xargs grep -L "confluent:tags" || echo "OK: todos os campos sensíveis taggeados"
```

---

## Template de relatório

```markdown
# Compliance Report — Bootcamp Confluent Cloud
**Data**: YYYY-MM-DD
**Revisor**: [nome]

## LGPD
- Campos PII catalogados (Stream Catalog): X/Y topics
- Tokenização PII (Flink): [PASS/FAIL]
- Retenção mínima aplicada: [PASS/FAIL]
- Direitos do titular demonstráveis: [PASS/FAIL]

## PCI-DSS
- PAN mascarado em todos os topics: [PASS/FAIL]
- ACLs restritas em topics de cartão: [PASS/FAIL]

## Acesso
- Least privilege por lab: [PASS/FAIL]
- API keys sem órfãs/vencidas: [PASS/FAIL]

## Ações
1. [ação] — Dono: [nome] — Prazo: [data]
```

---

## Best practices

1. **Taggear PII/PCI no momento do design do schema** — o hook bloqueia, mas o contrato vem antes
2. **Stream Governance como fonte do inventário** (catalog + lineage), não planilha
3. **Anonimizar no stream (shift-left)** — Flink SQL antes do consumo analítico/Tableflow
4. **Retenção curta por padrão** — labs regeneram dados por seed
5. **Demonstrar direitos do titular** no lab da Live 3 (acesso/eliminação)
6. **Dados sintéticos sempre** — é regra dura do bootcamp

---

**Última atualização**: 2026-07-11
