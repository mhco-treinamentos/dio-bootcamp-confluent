"""Tests for the producer profile selector — never touches a broker or the cloud."""

from __future__ import annotations

import pytest

from producers.config import build_producer_config, build_schema_registry_config
from producers.reliable import ProducerMetrics

_CLOUD_ENV = {
    "CC_BOOTSTRAP": "pkc-test.us-east1.gcp.confluent.cloud:9092",
    "CC_API_KEY": "test-key",
    "CC_API_SECRET": "test-secret",
    "SR_URL": "https://psrc-test.us-east1.gcp.confluent.cloud",
    "SR_API_KEY": "sr-key",
    "SR_API_SECRET": "sr-secret",
}


@pytest.fixture()
def cloud_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in _CLOUD_ENV.items():
        monkeypatch.setenv(key, value)


class TestProducerConfig:
    def test_local_profile_needs_no_credentials(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CC_BOOTSTRAP", raising=False)
        config = build_producer_config(profile="local")
        assert config["bootstrap.servers"] == "localhost:9092"
        assert "sasl.username" not in config

    def test_reliability_anchors_always_present(self) -> None:
        """acks=all + idempotência são âncoras do bootcamp — nos dois profiles."""
        config = build_producer_config(profile="local")
        assert config["acks"] == "all"
        assert config["enable.idempotence"] is True

    def test_cloud_profile_reads_env(self, cloud_env: None) -> None:
        config = build_producer_config(profile="cloud")
        assert config["bootstrap.servers"] == _CLOUD_ENV["CC_BOOTSTRAP"]
        assert config["security.protocol"] == "SASL_SSL"
        assert config["sasl.username"] == "test-key"

    def test_cloud_profile_missing_env_fails_fast(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CC_BOOTSTRAP", raising=False)
        with pytest.raises(KeyError):
            build_producer_config(profile="cloud")

    def test_profile_from_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KAFKA_PROFILE", "local")
        config = build_producer_config()
        assert config["bootstrap.servers"] == "localhost:9092"

    def test_defaults_to_local_without_any_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Botão ▶ da IDE sem env var nenhuma: cai no sandbox, não pede credencial."""
        monkeypatch.delenv("KAFKA_PROFILE", raising=False)
        monkeypatch.delenv("CC_BOOTSTRAP", raising=False)
        config = build_producer_config()
        assert config["bootstrap.servers"] == "localhost:9092"

    def test_defaults_to_cloud_when_credentials_present(
        self, monkeypatch: pytest.MonkeyPatch, cloud_env: None
    ) -> None:
        """Com um .env de nuvem carregado, o default vira cloud — sem surpresa."""
        monkeypatch.delenv("KAFKA_PROFILE", raising=False)
        config = build_producer_config()
        assert config["bootstrap.servers"] == _CLOUD_ENV["CC_BOOTSTRAP"]
        assert config["security.protocol"] == "SASL_SSL"

    def test_invalid_profile_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid KAFKA_PROFILE"):
            build_producer_config(profile="staging")

    def test_overrides_apply_last(self) -> None:
        config = build_producer_config(profile="local", overrides={"linger.ms": 0})
        assert config["linger.ms"] == 0


class TestSchemaRegistryConfig:
    def test_local_has_no_auth(self) -> None:
        config = build_schema_registry_config(profile="local")
        assert config == {"url": "http://localhost:8081"}

    def test_cloud_uses_basic_auth(self, cloud_env: None) -> None:
        config = build_schema_registry_config(profile="cloud")
        assert config["url"] == _CLOUD_ENV["SR_URL"]
        assert config["basic.auth.user.info"] == "sr-key:sr-secret"


class TestProducerMetrics:
    def test_counts_and_success_rate(self) -> None:
        metrics = ProducerMetrics()
        assert metrics.success_rate == 1.0

        class FakeMsg:
            def topic(self) -> str:
                return "t"

            def partition(self) -> int:
                return 0

            def offset(self) -> int:
                return 1

        metrics.on_delivery(None, FakeMsg())
        metrics.on_delivery("boom", FakeMsg())
        assert metrics.delivered == 1
        assert metrics.failed == 1
        assert metrics.success_rate == 0.5
