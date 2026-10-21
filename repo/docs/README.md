# Guia do aluno — Bootcamp Confluent Cloud 2026

> Estes documentos são para **quem está fazendo o bootcamp**: passo a passo, exercícios e gabaritos, aula por aula.
> Procurando o roteiro de gravação? → [demos/curso1/README.md](../demos/curso1/README.md)

Cada aula tem um guia próprio com o que fazer, o que você deve ver acontecendo e como conferir se acertou.

## Como usar

1. **Assista à aula** — estes guias assumem que você já viu o conteúdo; eles são a parte prática.
2. **Siga os passos na ordem** — cada um tem um entregável e um jeito de se autoavaliar.
3. **Confira no gabarito só depois de responder** — reconhecer a resposta certa não é o mesmo que produzi-la.

Aulas conceituais não exigem instalação nenhuma. Aulas com prática exigem o sandbox local (`make up`) — cada guia declara o que precisa logo no início.

## Trilha

| Curso | Tema | Aulas | Guias |
|---|---|---|---|
| 1 | Fundamentos de EDA e Kafka | 6 | ✅ [ver aulas](curso1/README.md) |
| 2 | Confluent Cloud, Schema Registry e Data Contracts | 6 | ✅ [ver aulas](curso2/README.md) |
| 3 | Kafka Connect e CDC | 5 | ✅ [ver aulas](curso3/README.md) |
| 4 | Apache Flink — processamento de streams | 4 | ✅ [ver aulas](curso4/README.md) |
| 5 | Lakehouse com Tableflow e Iceberg | 4 | *em construção* |
| 6 | Real-Time AI | 4 | *em construção* |
| 7 | Operação profissional | 5 | ✅ [ver aulas](curso7/README.md) |
| — | **Desafio Final** — pipeline fim a fim | — | ✅ [ver o desafio](../desafio-final/README.md) |

> 💡 O Curso 7 **não depende** dos Cursos 5 e 6 — ele opera o que os Cursos 1 a 4
> construíram. Pode ser feito na sequência do Curso 4.

## Antes da primeira aula prática

**macOS / Linux**
```bash
make install
make up
make status
```

`make install` cria o venv e instala as dependências (uma vez); `make up` sobe o
sandbox local — Kafka + Schema Registry (~30s); `make status` confirma que o broker
responde.

**Windows (PowerShell)** — sem `make`, os mesmos passos por extenso:
```powershell
py -3.11 -m venv .venv                                        # uma vez
.venv\Scripts\pip install --upgrade pip
.venv\Scripts\pip install -e ".[dev,cdc]"
docker compose -f deployment/docker/docker-compose.yml up -d  # sobe o sandbox
docker compose -f deployment/docker/docker-compose.yml ps     # confirma: Up
```

> 🪟 Nas aulas práticas do Curso 1, todo comando multi-linha aparece em dupla:
> **macOS / Linux** (com `\` de continuação) e **Windows (PowerShell)** (uma linha
> só). Usuários de **Git Bash** ou **WSL** podem seguir as versões macOS/Linux.

Prefere trabalhar pela IDE? → [PYCHARM.md](../PYCHARM.md) tem 41 configurações prontas, com o botão ▶.

## Convenções destes guias

| Marca | Significado |
|---|---|
| ⏱ | Tempo estimado do exercício (não da aula gravada) |
| 📄 | Exercício em papel — sem Docker, sem terminal |
| 💻 | Prática no sandbox local |
| 💡 | Dica ou atalho |
| ⚠️ | Erro comum ou pegadinha |

Prosa em português; nomes de código, tópicos e comandos em inglês — como no resto do repositório.

## Sobre o domínio dos exemplos

Todos os guias usam **payments**: transações de cartão e PIX, contas, antifraude. É o mesmo domínio do começo ao fim — a transação que você classifica no papel na Aula 1 é a que aparece no terminal na Aula 5, vira CDC no Curso 3 e é analisada por Flink no Curso 4.

Algumas aulas gravadas usam exemplos de e-commerce (pedidos). O conceito é idêntico; só muda o domínio.

---

## Nota para quem edita estes guias

Existem **dois** conjuntos de documentos, com públicos diferentes:

| Documento | Público | Papel |
|---|---|---|
| `docs/dio-bootcamp-cursoN/` (raiz do projeto) | instrutor | roteiro de gravação: falas, tempos, o que apontar na tela |
| `repo/docs/cursoN/aulaM/` (aqui) | **aluno** | passos acionáveis, exercícios, gabaritos |
| `repo/demos/curso1/README.md` | instrutor | roteiro de execução das demos ao vivo |

**Regra para não duplicar**: copie *artefatos de consulta* (tabelas, diagramas, referências) — o aluno precisa deles à mão durante o exercício. **Nunca** copie *prosa explicativa* (analogias, narrativas): ela vive na aula gravada, e texto duplicado inevitavelmente diverge. Quando faltar contexto, aponte para a aula em vez de reescrevê-la.
