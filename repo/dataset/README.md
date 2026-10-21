# dataset/ — os dados do bootcamp

Tudo aqui é **100% sintético e determinístico** (`seed=42`): todo aluno vê exatamente
os mesmos 2.820 registros — é isso que permite os guias mostrarem outputs esperados
que batem com o seu terminal.

| Pasta | O quê |
|---|---|
| [`generators/`](generators/) | Geradores Python do domínio payments (customers, accounts, cards, merchants, transactions) com formatos brasileiros válidos (CPF, CNPJ, MCC) e PAN sempre mascarado |
| [`neon/`](neon/) | O "banco legado" do Curso 3: DDL, seed gerado, setup de CDC (role + publication) e teardown para o Postgres no Neon |

Regenerar o seed: `python -m dataset.neon.generate_seed` — o arquivo resultante deve
ser **idêntico** ao versionado (determinismo é contrato).
