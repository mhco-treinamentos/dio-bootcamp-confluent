"""Generic helpers shared by domain generators (Brazilian formats, distributions)."""

from dataset.generators.common.brazilian_formats import (
    generate_cnpj,
    generate_cpf,
    validate_cnpj,
    validate_cpf,
)

__all__ = ["generate_cnpj", "generate_cpf", "validate_cnpj", "validate_cpf"]
