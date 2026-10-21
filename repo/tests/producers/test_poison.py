"""Tests for the serialization poison payloads (sem tocar broker)."""

from __future__ import annotations

import json
import struct

from producers.poison import SerializationPoison


class TestSerializationPoison:
    def test_not_avro_json_lacks_magic_byte(self) -> None:
        """Payload JSON não tem o magic byte 0x00 do wire format Confluent."""
        payload = SerializationPoison.NOT_AVRO_JSON.value
        assert payload[0:1] != b"\x00"
        # É JSON válido — o veneno perfeito: parseia como texto, quebra como Avro
        assert json.loads(payload)["transaction_id"] == "poison-json"

    def test_truncated_avro_has_valid_header_bad_body(self) -> None:
        """Magic byte + schema id válidos, corpo corrompido — falha no meio."""
        payload = SerializationPoison.TRUNCATED_AVRO.value
        magic, schema_id = struct.unpack(">bI", payload[:5])
        assert magic == 0
        assert schema_id == 1
        assert payload[5:] == b"\xde\xad\xbe\xef"

    def test_empty_payload(self) -> None:
        assert SerializationPoison.EMPTY.value == b""

    def test_poisons_are_distinct(self) -> None:
        values = [p.value for p in SerializationPoison]
        assert len(values) == len(set(values))
