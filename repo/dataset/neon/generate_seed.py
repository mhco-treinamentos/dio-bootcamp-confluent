"""Generate the deterministic seed SQL (``02_seed.sql``) for the Neon dataset.

Uses the ``dataset.generators.payments`` pool chain with ``seed=42`` and the
fixed :data:`~dataset.generators.common.temporal.REFERENCE_TIME` anchor, so the
checked-in seed is reproducible byte a byte: every student loads the same
"legacy database".

Usage
-----
::

    python -m dataset.neon.generate_seed

Volumes stay well under the labs limit (>10k records per section burns time
and credits — .claude/rules/labs.md).
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from dataset.generators.payments import (
    AccountGenerator,
    AccountGeneratorConfig,
    CardGenerator,
    CardGeneratorConfig,
    CustomerGenerator,
    CustomerGeneratorConfig,
    MerchantGenerator,
    MerchantGeneratorConfig,
    TransactionGenerator,
    TransactionGeneratorConfig,
)

logger = logging.getLogger(__name__)

SEED = 42
N_CUSTOMERS = 200
N_ACCOUNTS = 240
N_CARDS = 320
N_MERCHANTS = 60
N_TRANSACTIONS = 2_000
BATCH_SIZE = 100

OUTPUT_PATH = Path(__file__).resolve().parent / "02_seed.sql"

_HEADER = """\
-- =============================================================================
-- Bootcamp Confluent Cloud 2026 — Seed do "banco legado" (Neon Postgres)
--
-- ⚠️ ARQUIVO GERADO — não edite na mão. Para regenerar:
--   python -m dataset.neon.generate_seed   (seed=42, determinístico)
--
-- Rode DEPOIS do 01_ddl.sql. Idempotente: ON CONFLICT DO NOTHING.
-- Estes registros são o que o conector CDC captura como snapshot inicial
-- (op='r') na Aula 2 do Curso 3.
--
-- Volumes: {n_customers} customers, {n_accounts} accounts, {n_cards} cards,
--          {n_merchants} merchants, {n_transactions} transactions
-- =============================================================================

BEGIN;
"""

_FOOTER = """\
COMMIT;

-- Verificação — esperado:
-- {n_customers} / {n_accounts} / {n_cards} / {n_merchants} / {n_transactions}
-- SELECT (SELECT count(*) FROM payments.customers)    AS customers,
--        (SELECT count(*) FROM payments.accounts)     AS accounts,
--        (SELECT count(*) FROM payments.cards)        AS cards,
--        (SELECT count(*) FROM payments.merchants)    AS merchants,
--        (SELECT count(*) FROM payments.transactions) AS transactions;
"""


def _sql(value: Any) -> str:
    """Render a Python value as a SQL literal (safe for synthetic data)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return f"{value:.2f}"
    if isinstance(value, datetime):
        return f"'{value.isoformat()}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    return "'" + str(value).replace("'", "''") + "'"


def _insert_batches(
    table: str,
    columns: list[str],
    rows: list[list[Any]],
    conflict_key: str,
) -> list[str]:
    """Build multi-row INSERT statements (``BATCH_SIZE`` rows each)."""
    statements = []
    col_list = ", ".join(columns)
    for start in range(0, len(rows), BATCH_SIZE):
        chunk = rows[start : start + BATCH_SIZE]
        values = ",\n".join("  (" + ", ".join(_sql(v) for v in row) + ")" for row in chunk)
        statements.append(
            f"INSERT INTO {table} ({col_list}) VALUES\n{values}\n"
            f"ON CONFLICT ({conflict_key}) DO NOTHING;\n"
        )
    return statements


def build_pools() -> dict[str, list[dict[str, Any]]]:
    """Generate the full entity pool chain (referentially consistent, seed=42).

    Returns
    -------
    dict[str, list[dict[str, Any]]]
        Pools keyed by entity name: customers, accounts, cards, merchants,
        transactions.
    """
    customers = list(CustomerGenerator(CustomerGeneratorConfig(seed=SEED)).generate(N_CUSTOMERS))
    accounts = list(
        AccountGenerator(customers, AccountGeneratorConfig(seed=SEED)).generate(N_ACCOUNTS)
    )
    cards = list(
        CardGenerator(accounts, customers, CardGeneratorConfig(seed=SEED)).generate(N_CARDS)
    )
    merchants = list(MerchantGenerator(MerchantGeneratorConfig(seed=SEED)).generate(N_MERCHANTS))
    transactions = list(
        TransactionGenerator(
            customers, accounts, cards, merchants, TransactionGeneratorConfig(seed=SEED)
        ).generate(N_TRANSACTIONS)
    )
    return {
        "customers": customers,
        "accounts": accounts,
        "cards": cards,
        "merchants": merchants,
        "transactions": transactions,
    }


def render_seed_sql(pools: dict[str, list[dict[str, Any]]]) -> str:
    """Render the full seed SQL script for the given pools.

    Parameters
    ----------
    pools : dict[str, list[dict[str, Any]]]
        Output of :func:`build_pools`.

    Returns
    -------
    str
        Complete SQL script (transactionally wrapped, idempotent).
    """
    counts = {
        "n_customers": len(pools["customers"]),
        "n_accounts": len(pools["accounts"]),
        "n_cards": len(pools["cards"]),
        "n_merchants": len(pools["merchants"]),
        "n_transactions": len(pools["transactions"]),
    }
    parts: list[str] = [_HEADER.format(**counts)]

    parts.append(
        "\n-- customers -----------------------------------------------------------------\n"
    )
    parts.extend(
        _insert_batches(
            "payments.customers",
            [
                "customer_id",
                "cpf",
                "nome",
                "data_nascimento",
                "email",
                "telefone",
                "logradouro",
                "numero",
                "complemento",
                "bairro",
                "cidade",
                "uf",
                "cep",
                "pais",
                "status",
                "created_at",
                "updated_at",
            ],
            [
                [
                    c["customer_id"],
                    c["cpf"],
                    c["nome"],
                    c["data_nascimento"],
                    c["email"],
                    c["telefone"],
                    c["endereco"]["logradouro"],
                    c["endereco"]["numero"],
                    c["endereco"]["complemento"],
                    c["endereco"]["bairro"],
                    c["endereco"]["cidade"],
                    c["endereco"]["uf"],
                    c["endereco"]["cep"],
                    c["endereco"]["pais"],
                    c["status"],
                    c["created_at"],
                    c["updated_at"],
                ]
                for c in pools["customers"]
            ],
            conflict_key="customer_id",
        )
    )

    parts.append(
        "\n-- accounts ------------------------------------------------------------------\n"
    )
    parts.extend(
        _insert_batches(
            "payments.accounts",
            [
                "account_id",
                "customer_id",
                "agencia",
                "conta",
                "tier",
                "balance",
                "overdraft_limit",
                "status",
                "opened_at",
                "closed_at",
                "created_at",
                "updated_at",
            ],
            [
                [
                    a["account_id"],
                    a["customer_id"],
                    a["agencia"],
                    a["conta"],
                    a["tier"],
                    a["balance"],
                    a["overdraft_limit"],
                    a["status"],
                    a["opened_at"],
                    a["closed_at"],
                    a["created_at"],
                    a["updated_at"],
                ]
                for a in pools["accounts"]
            ],
            conflict_key="account_id",
        )
    )

    parts.append(
        "\n-- cards ---------------------------------------------------------------------\n"
    )
    parts.extend(
        _insert_batches(
            "payments.cards",
            [
                "card_id",
                "account_id",
                "pan_masked",
                "brand",
                "card_type",
                "credit_limit",
                "uf_emissao",
                "status",
                "issued_at",
                "expires_at",
                "created_at",
                "updated_at",
            ],
            [
                [
                    c["card_id"],
                    c["account_id"],
                    c["pan_masked"],
                    c["brand"],
                    c["card_type"],
                    c["credit_limit"],
                    c["uf_emissao"],
                    c["status"],
                    c["issued_at"],
                    c["expires_at"],
                    c["created_at"],
                    c["updated_at"],
                ]
                for c in pools["cards"]
            ],
            conflict_key="card_id",
        )
    )

    parts.append(
        "\n-- merchants ----------------------------------------------------------------\n"
    )
    parts.extend(
        _insert_batches(
            "payments.merchants",
            [
                "merchant_id",
                "cnpj",
                "nome_fantasia",
                "mcc",
                "uf",
                "logradouro",
                "numero",
                "complemento",
                "bairro",
                "cidade",
                "cep",
                "pais",
                "status",
                "created_at",
                "updated_at",
            ],
            [
                [
                    m["merchant_id"],
                    m["cnpj"],
                    m["nome_fantasia"],
                    m["mcc"],
                    m["uf"],
                    m["endereco"]["logradouro"],
                    m["endereco"]["numero"],
                    m["endereco"]["complemento"],
                    m["endereco"]["bairro"],
                    m["endereco"]["cidade"],
                    m["endereco"]["cep"],
                    m["endereco"]["pais"],
                    m["status"],
                    m["created_at"],
                    m["updated_at"],
                ]
                for m in pools["merchants"]
            ],
            conflict_key="merchant_id",
        )
    )

    parts.append(
        "\n-- transactions --------------------------------------------------------------\n"
    )
    parts.extend(
        _insert_batches(
            "payments.transactions",
            [
                "transaction_id",
                "account_id",
                "card_id",
                "merchant_id",
                "channel",
                "amount",
                "mcc",
                "uf_transacao",
                "status",
                "decline_reason",
                "fraud_flag",
                "fraud_rule",
                "created_at",
                "authorized_at",
                "settled_at",
            ],
            [
                [
                    t["transaction_id"],
                    t["account_id"],
                    t["card_id"],
                    t["merchant_id"],
                    t["channel"],
                    t["amount"],
                    t["mcc"],
                    t["uf_transacao"],
                    t["status"],
                    t["decline_reason"],
                    t["fraud_flag"],
                    t["fraud_rule"],
                    t["created_at"],
                    t["authorized_at"],
                    t["settled_at"],
                ]
                for t in pools["transactions"]
            ],
            conflict_key="transaction_id",
        )
    )

    parts.append("\n" + _FOOTER.format(**counts))
    return "".join(parts)


def main() -> None:
    """Generate and write ``02_seed.sql``."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    pools = build_pools()
    sql = render_seed_sql(pools)
    OUTPUT_PATH.write_text(sql, encoding="utf-8")
    logger.info(
        "Wrote %s (%d bytes, %d transactions)",
        OUTPUT_PATH,
        len(sql.encode("utf-8")),
        len(pools["transactions"]),
    )


if __name__ == "__main__":
    main()
