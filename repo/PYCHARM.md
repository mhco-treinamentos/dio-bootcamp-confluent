# Rodando os scripts no PyCharm

Todos os producers, consumers e demos rodam pelo **botão ▶** do PyCharm, sem configuração manual.

## Setup inicial (uma vez)

1. **Abra a pasta `repo/` como projeto** — não a pasta pai. O PyCharm precisa que a raiz do projeto seja onde está o `pyproject.toml`.

2. **Aponte o interpretador para o venv:**
   `Settings → Project → Python Interpreter → Add Local Interpreter → Existing` → selecione `repo/.venv/bin/python`

   Se o venv ainda não existe: `make install` no terminal.

3. **Suba o Kafka** (fora do PyCharm): `make up`

Pronto. As **41 run configurations** em `.run/` aparecem no dropdown ao lado do ▶.

## O que já vem configurado

| Item | Valor | Por quê |
|---|---|---|
| Working directory | raiz do projeto | imports `from producers...` funcionam |
| `KAFKA_PROFILE` | `local` | aponta para o sandbox Docker, não para a nuvem |
| `PYTHONUNBUFFERED` | `1` | output aparece na hora, não em blocos |
| Emulate terminal | ligado | as cores das demos renderizam |
| Run as module | sim (`-m`) | equivalente ao que o `make` faz |

## As configurations disponíveis

**Progressão de clients** (Curso 2 em diante):
- `Producer 01 · hello_kafka` → `02 · json` → `03 · avro`
- `Consumer 01 · hello` → `02 · json` → `03 · avro` → `04 · reliable` → `05 · DLQ`

**API por dentro (Aulas 4–5)** — as configs `Aula 4 API · *` e `Aula 5 API · *` rodam os scripts de anatomia da API (`demos/curso1/aula*_api_*.py`). Para o rebalance da Aula 4, rode `consumer A` e `consumer B` em abas separadas; para o crash da Aula 5, rode a MESMA config duas vezes e compare os outputs.

**Demos do Curso 1** — nomeadas por aula e cena (`Aula 2 · consumidor A`, `Aula 4 · lag do grupo`, etc.). Para as cenas que exigem terminais simultâneos (Aula 2 e 4), rode cada configuration numa aba de execução separada: o PyCharm permite múltiplas execuções simultâneas com o botão ▶ (ou `Ctrl+Shift+F10` em cada uma).

## Funciona sem nenhuma env var

Os scripts detectam o ambiente sozinhos: **sem `CC_BOOTSTRAP` definida, o profile é `local`**. Ou seja, o ▶ aponta para o Docker por padrão, e a nuvem exige escolha explícita (créditos custam dinheiro).

Os producers de exemplo também **criam o topic** se ele não existir — no sandbox o auto-create está desligado de propósito (a criação explícita com N partições é a lição da Aula 3).

## Problemas comuns

### `ModuleNotFoundError: No module named 'producers'`
O projeto foi aberto na pasta errada, ou o venv não está instalado como editable. Rode:
```bash
cd repo && .venv/bin/pip install -e ".[dev]"
```

### O script roda mas nada é entregue
O Kafka não está no ar. `make status` deve mostrar os containers `Up`. Se não: `make up`.

### As cores saem como `[96m` no console
Falta o "Emulate terminal in output console" (já vem ligado nas configurations de `.run/`; se criar uma nova à mão, marque essa opção).

### Quero rodar contra o Confluent Cloud
Preencha o `.env` (veja `.env.example`) e defina `KAFKA_PROFILE=cloud` na configuration — ou deixe o `CC_BOOTSTRAP` no ambiente, que a detecção automática assume a nuvem.
