# schemas/payments/ — os contratos de dados

Schemas Avro do domínio payments, namespace `com.bootcamp.payments.*`. São o
**contrato** entre quem produz e quem consome — o Curso 2 (Aula 3) mostra o Schema
Registry aceitando e **recusando** evoluções destes arquivos.

Convenções (detalhes em [.claude/rules/schemas.md](../../../.claude/rules/schemas.md)):

- **Subject**: `{topic}-value` (TopicNameStrategy); compatibilidade **BACKWARD**
- **Dinheiro**: `bytes` + `logicalType: decimal(15,2)` — nunca float
- **Tags de governança**: campos sensíveis carregam `confluent:tags` — `PII`/`LGPD`
  (CPF, nome, endereço), `PCI`/`PCI_DSS` (PAN mascarado), `FIN` (valores)
- **Proibido em qualquer tópico**, mesmo sintético: PAN completo, CVV, PIN
