# Data Modeling Reference

> **Domain data modeling patterns for the workshop payments data generation**

## Domain examples (legacy: health insurance; current workshop uses payments)

### Membro (Member) Model
```python
@dataclass
class Membro:
    """Health plan member (beneficiario)."""

    membro_id: str               # UUID
    cpf: str                     # PII
    cns: str                     # Cartão Nacional de Saúde (PII)
    nome: str                    # PII
    data_nascimento: date
    sexo: str                    # M, F
    email: str | None            # PII
    telefone: str | None         # PII
    plano_id: str                # FK to Plano
    tipo_titular: str            # TITULAR, DEPENDENTE
    titular_id: str | None       # FK to Membro (if dependente)
    status: str = "ATIVO"        # ATIVO, SUSPENSO, CANCELADO, CARENCIA
    data_adesao: date
    data_cancelamento: date | None = None
    endereco: Endereco
    created_at: datetime
    updated_at: datetime
```

### Plano (Plan) Model
```python
@dataclass
class Plano:
    """Domain entity example (legacy: health insurance plan)."""

    plano_id: str
    nome: str                    # "Premium", "Basic"
    tipo: str                    # INDIVIDUAL, FAMILIAR, EMPRESARIAL, ADESAO
    abrangencia: str             # MUNICIPAL, ESTADUAL, NACIONAL
    segmentacao: str             # AMBULATORIAL, HOSPITALAR, OBSTETRICO, REFERENCIA
    ans_codigo: str              # ANS registry code
    valor_base: Decimal          # Base monthly premium
    coparticipacao: bool
    created_at: datetime
```

### Guia (Authorization Guide) Model
```python
@dataclass
class Guia:
    """TISS authorization guide."""

    guia_id: str                 # UUID
    numero_guia: str             # TISS guide number (format: XXXXXXXXXX)
    membro_id: str               # FK to Membro
    prestador_id: str            # FK to Prestador
    tipo_guia: str               # CONSULTA, SP_SADT, INTERNACAO, ODONTOLOGICA, HONORARIOS
    status: str = "SOLICITADA"   # SOLICITADA→EM_ANALISE→AUTORIZADA→EXECUTADA→FATURADA
    cid_10: str | None           # PHI - ICD-10 code
    valor_autorizado: Decimal
    valor_executado: Decimal | None
    valor_glosa: Decimal | None
    dt_solicitacao: datetime
    dt_autorizacao: datetime | None
    dt_execucao: datetime | None
    senha_autorizacao: str | None
    created_at: datetime
    updated_at: datetime
```

### GuiaItem (Guide Line Item) Model
```python
@dataclass
class GuiaItem:
    """Individual procedure within a TISS guide."""

    guia_item_id: str
    guia_id: str                 # FK to Guia
    codigo_tuss: str             # 8-digit TUSS code
    descricao_tuss: str
    quantidade: int
    valor_unitario: Decimal
    valor_total: Decimal
    status: str = "SOLICITADO"
    dt_execucao: datetime | None
    created_at: datetime
```

### Prestador (Provider) Model
```python
@dataclass
class Prestador:
    """Domain entity example (legacy: healthcare provider)."""

    prestador_id: str
    cnpj: str
    nome: str
    tipo: str                    # CLINICA, HOSPITAL, LABORATORIO, CONSULTORIO
    especialidade: str           # CARDIOLOGIA, ORTOPEDIA, etc.
    crm: str | None              # Medical registry (for individuals)
    cnes: str                    # National Health Establishment Code
    endereco: Endereco
    status: str = "ATIVO"
    created_at: datetime
    updated_at: datetime
```

### Mensalidade (Monthly Premium) Model
```python
@dataclass
class Mensalidade:
    """Monthly premium payment."""

    mensalidade_id: str
    membro_id: str               # FK to Membro
    plano_id: str                # FK to Plano
    competencia: str             # "2026-01", "2026-02"
    valor: Decimal
    dt_vencimento: date
    dt_pagamento: date | None
    status: str = "PENDENTE"     # PENDENTE, PAGO, ATRASADO, INADIMPLENTE
    created_at: datetime
    updated_at: datetime
```

### Fatura (Invoice) Model
```python
@dataclass
class Fatura:
    """Provider invoice for services rendered."""

    fatura_id: str
    prestador_id: str            # FK to Prestador
    competencia: str             # "2026-01"
    valor_total: Decimal
    valor_glosa: Decimal
    valor_liquido: Decimal
    quantidade_guias: int
    status: str = "ABERTA"       # ABERTA, PROCESSADA, PAGA, CONTESTADA
    dt_emissao: date
    dt_pagamento: date | None
    created_at: datetime
```

## Common Models

### Endereco (Address) Model
```python
@dataclass
class Endereco:
    """Brazilian address."""

    logradouro: str
    numero: str
    complemento: str = ""
    bairro: str = ""
    cidade: str = ""
    uf: str = ""                 # 2-letter code (MG, SP, etc.)
    cep: str = ""                # CEP format: XXXXX-XXX
    pais: str = "BR"
```

## Relationships

### Domain examples (legacy: health insurance)
```
Membro 1:N Dependente (via titular_id)
Membro N:1 Plano
Membro 1:N Guia
Membro 1:N Mensalidade
Guia 1:N GuiaItem
Guia N:1 Prestador
Prestador 1:N Contrato
Prestador 1:N Fatura
Fatura 1:N FaturaItem (linked to GuiaItem)
```

## Event Modeling

### Event Envelope
```python
@dataclass
class Event:
    """Standard event envelope."""

    event_id: str
    event_type: str              # entity.action (e.g., guia.autorizada)
    event_time: datetime
    source: str                  # Oracle schema that generated
    subject: str                 # Entity ID affected
    data: dict                   # Event payload
    metadata: dict = field(default_factory=dict)
```

### Event Types — Health Insurance (legacy example)

#### Member Events
- `membro.cadastrado` — New member enrolled
- `membro.ativado` — Carencia period completed
- `membro.suspenso` — Payment default
- `membro.reativado` — Payment regularized
- `membro.cancelado` — Plan cancelled

#### Authorization Events
- `guia.solicitada` — Guide submitted
- `guia.autorizada` — Guide approved
- `guia.negada` — Guide denied
- `guia.executada` — Procedure performed
- `guia.faturada` — Claim submitted
- `guia.glosada` — Claim denied/reduced

#### Billing Events
- `mensalidade.gerada` — Monthly bill generated
- `mensalidade.paga` — Premium paid
- `mensalidade.atrasada` — Payment overdue
- `fatura.emitida` — Provider invoice issued
- `fatura.paga` — Provider paid
