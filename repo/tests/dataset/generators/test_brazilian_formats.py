"""Tests for Brazilian format generators/validators (property-based included)."""

from __future__ import annotations

import random

from hypothesis import given
from hypothesis import strategies as st

from dataset.generators.common.brazilian_formats import (
    AREA_CODES,
    UF_WEIGHTS,
    generate_cep,
    generate_cnpj,
    generate_cpf,
    generate_phone,
    pick_uf,
    validate_cnpj,
    validate_cpf,
)


class TestCPF:
    @given(seed=st.integers(min_value=0, max_value=10_000))
    def test_generated_cpf_is_valid(self, seed: int) -> None:
        """Todo CPF gerado precisa passar na validação de check digit."""
        cpf = generate_cpf(random.Random(seed))
        assert len(cpf) == 11
        assert validate_cpf(cpf)

    def test_rejects_wrong_check_digit(self) -> None:
        cpf = generate_cpf(random.Random(42))
        bad = cpf[:-1] + str((int(cpf[-1]) + 1) % 10)
        assert not validate_cpf(bad)

    def test_rejects_repeated_digits_and_garbage(self) -> None:
        assert not validate_cpf("11111111111")
        assert not validate_cpf("123")
        assert not validate_cpf("abcdefghijk")

    def test_accepts_formatted_input(self) -> None:
        cpf = generate_cpf(random.Random(1))
        formatted = f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"
        assert validate_cpf(formatted)


class TestCNPJ:
    @given(seed=st.integers(min_value=0, max_value=10_000))
    def test_generated_cnpj_is_valid(self, seed: int) -> None:
        """Todo CNPJ gerado precisa passar na validação de check digit."""
        cnpj = generate_cnpj(random.Random(seed))
        assert len(cnpj) == 14
        assert validate_cnpj(cnpj)

    def test_branch_is_0001(self) -> None:
        cnpj = generate_cnpj(random.Random(42))
        assert cnpj[8:12] == "0001"

    def test_rejects_wrong_check_digit(self) -> None:
        cnpj = generate_cnpj(random.Random(42))
        bad = cnpj[:-1] + str((int(cnpj[-1]) + 1) % 10)
        assert not validate_cnpj(bad)


class TestPhoneCepUf:
    def test_phone_format(self) -> None:
        rng = random.Random(42)
        for _ in range(50):
            phone = generate_phone(rng)
            assert phone.startswith("+55")
            assert phone[3:5] in AREA_CODES
            assert len(phone) == 3 + 2 + 9  # +55 DDD 9XXXXXXXX
            assert phone[5] == "9"  # celular

    def test_cep_has_8_digits(self) -> None:
        rng = random.Random(42)
        for _ in range(50):
            cep = generate_cep(rng)
            assert len(cep) == 8
            assert cep.isdigit()

    def test_pick_uf_returns_known_uf(self) -> None:
        rng = random.Random(42)
        for _ in range(100):
            assert pick_uf(rng) in UF_WEIGHTS

    def test_uf_distribution_favors_sp(self) -> None:
        """SP tem o maior peso — deve dominar uma amostra grande."""
        rng = random.Random(42)
        sample = [pick_uf(rng) for _ in range(2_000)]
        assert sample.count("SP") / len(sample) > 0.15
