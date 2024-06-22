from __future__ import annotations

import datetime
import gzip
from collections.abc import Generator
from pathlib import Path
from typing import Optional

import pytest

from valens import app, database
from valens.models import OpenFoodFactsEntry
from valens.nutrition import openfoodfacts
from valens.nutrition.openfoodfacts import (
    InvalidDataError,
    _convert_entry,
    _convert_nutrient,
    _valid_ean8,
    _valid_ean13,
    _valid_ean_country_code,
    import_url,
)


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        (
            """
            INVALID JSON
            """,
            r"Expecting value: line 2 column 13 \(char 13\)",
        ),
        (
            """
            { "code": "1234", "no_nutrition_data": "on" }
            """,
            "no nutrition data",
        ),
        (
            """
            { "code": "1234" }
            """,
            "no identifier",
        ),
        (
            """
            { "id": "0", "code": "1234" }
            """,
            r"invalid identifier \(0\)",
        ),
        (
            """
            { "id": "1", "code": "1234" }
            """,
            "no creation date",
        ),
        (
            """
            { "id": "1", "code": "1234", "created_t": 1234567890 }
            """,
            "no product name",
        ),
        (
            """
            { "id": "1", "code": "1234", "created_t": 1234567890, "product_name": "Banana" }
            """,
            "no codes tags",
        ),
        (
            # Obsolete
            """
            { "id": "1",
              "code": "1234",
              "created_t": 123,
              "product_name": "Banana",
              "codes_tags": [ "code-13" ],
              "obsolete": "on"
            }
            """,
            "obsolete entry",
        ),
        (
            # Neither EAN-8 nor EAN-13
            """
            { "id": "1",
              "code": "1234",
              "created_t": 1234567890,
              "product_name": "Banana",
              "codes_tags": [ "code-99" ]
            }
            """,
            "no supported code tag found",
        ),
        (
            """
            { "id": "1",
              "code": "invalid",
              "created_t": 1234567890,
              "product_name": "Banana",
              "codes_tags": [ "code-8" ]
            }
            """,
            "invalid EAN-8 code",
        ),
        (
            """
            { "id": "1",
              "code": "invalid",
              "created_t": 1234567890,
              "product_name": "Banana",
              "codes_tags": [ "code-13" ]
            }
            """,
            "invalid EAN-13 code",
        ),
        (
            """
            { "id": "1",
              "code": "4017100290008",
              "created_t": 1234567890,
              "product_name": "Banana",
              "codes_tags": [ "code-13" ]
            }
            """,
            "no nutriments present",
        ),
        (
            """
            { "id": "1",
              "code": "4017100290008",
              "created_t": 1234567890,
              "product_name": "Banana",
              "codes_tags": [ "code-13" ],
              "serving_quantity": 100,
              "serving_quantity_unit": "%",
              "nutriments": { }
            }
            """,
            "serving_quantity in percent, but no product_quantity",
        ),
        (
            """
            { "id": "1",
              "code": "4017100290008",
              "created_t": 1234567890,
              "product_name": "Banana",
              "codes_tags": [ "code-13" ],
              "serving_quantity": 1,
              "serving_quantity_unit": "invalid",
              "nutriments": { }
            }
            """,
            "unsupported serving quantity unit: invalid",
        ),
        (
            """
            { "id": "1",
              "code": "4017100290008",
              "created_t": 1234567890,
              "product_name": "Banana",
              "codes_tags": [ "code-13" ],
              "nutrition_data_per": "serving",
              "nutriments": { }
            }
            """,
            "nutrition data per serving, but no serving quantity",
        ),
        (
            """
            { "id": "1",
              "code": "4017100290008",
              "created_t": 1234567890,
              "product_name": "Banana",
              "codes_tags": [ "code-13" ],
              "nutriments": { }
            }
            """,
            "all nutrition data is zero",
        ),
        (
            """
            { "id": "1",
              "code": "4017100290008",
              "created_t": 1234567890,
              "product_name": "Banana",
              "codes_tags": [ "code-13" ],
              "nutriments": {
                "alcohol": 0.1
              }
            }
            """,
            "alcohol has no unit",
        ),
        (
            """
            { "id": "1",
              "code": "4017100290008",
              "created_t": 1234567890,
              "product_name": "Banana",
              "codes_tags": [ "code-13" ],
              "nutriments": {
                "alcohol": 0.1,
                "alcohol_unit": "invalid"
              }
            }
            """,
            "invalid alcohol unit: invalid",
        ),
    ],
)
def test_convert_entry_error(data: str, expected: str) -> None:
    with pytest.raises(InvalidDataError, match=rf"^{expected}$"):
        _convert_entry(data.encode())


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        (
            """
            { "id": "1",
              "code": "4017100290008",
              "created_t": 1234567890,
              "product_name": "Banana",
              "codes_tags": [ "code-13" ],
              "nutriments": {
                "alcohol": 5.0,
                "alcohol_unit": "% vol"
              }
            }
            """,
            OpenFoodFactsEntry(
                code="4017100290008",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Banana",
                alcohol=5 * 0.789,
            ),
        ),
        (
            """
            { "id": "10",
              "code": "4017100290008",
              "created_t": 1234567890,
              "product_name": "Banana",
              "codes_tags": [ "code-13" ],
              "nutriments": {
                "alcohol": 3.0,
                "alcohol_unit": "g"
              }
            }
            """,
            OpenFoodFactsEntry(
                code="4017100290008",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Banana",
                alcohol=3.0,
            ),
        ),
        (
            """
            { "id": "2",
              "code": "44000271",
              "created_t": 1234567890,
              "product_name": "Apple",
              "codes_tags": [ "code-8" ],
              "nutriments": {
                "energy-kj": 123,
                "calcium": 1,
                "calcium_unit": "mg"
              }
            }
            """,
            OpenFoodFactsEntry(
                code="44000271",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Apple",
                energy=123 * 0.23900574,
                calcium=0.001,
            ),
        ),
        (
            """
            { "id": "3",
              "code": "44000271",
              "created_t": 1234567890,
              "product_name": "Apple",
              "codes_tags": [ "code-8" ],
              "serving_quantity": 50,
              "serving_quantity_unit": "g",
              "nutriments": {
                "energy-kj": 123,
                "calcium": 1,
                "calcium_unit": "mg"
              }
            }
            """,
            OpenFoodFactsEntry(
                code="44000271",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Apple",
                serving_quantity=50.0,
                energy=123 * 0.23900574,
                calcium=0.001,
            ),
        ),
        (
            """
            { "id": "4",
              "code": "44000271",
              "created_t": 1234567890,
              "product_name": "Apple",
              "codes_tags": [ "code-8" ],
              "product_quantity": 200,
              "serving_quantity": 50,
              "serving_quantity_unit": "%",
              "nutriments": {
                "energy-kj": 123,
                "calcium": 1,
                "calcium_unit": "mg"
              }
            }
            """,
            OpenFoodFactsEntry(
                code="44000271",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Apple",
                serving_quantity=100.0,
                quantity=200.0,
                energy=123 * 0.23900574,
                calcium=0.001,
            ),
        ),
        (
            """
            { "id": "5",
              "code": "44000271",
              "created_t": 1234567890,
              "product_name": "Apple",
              "codes_tags": [ "code-8" ],
              "serving_quantity": 50,
              "nutrition_data_per": "serving",
              "nutriments": {
                "energy-kj": 123,
                "calcium": 1,
                "calcium_unit": "mg"
              }
            }
            """,
            OpenFoodFactsEntry(
                code="44000271",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Apple",
                serving_quantity=50.0,
                energy=246 * 0.23900574,
                calcium=0.002,
            ),
        ),
        (
            """
            { "id": "6",
              "code": "44000271",
              "created_t": 1234567890,
              "product_name": "Apple",
              "codes_tags": [ "code-8" ],
              "nutriments": {
                "energy-kcal": 123,
                "calcium": 1,
                "calcium_unit": "mg"
              }
            }
            """,
            OpenFoodFactsEntry(
                code="44000271",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Apple",
                energy=123.0,
                calcium=0.001,
            ),
        ),
        (
            """
            { "id": "7",
              "code": "44000271",
              "created_t": 1234567890,
              "product_name": "Apple",
              "codes_tags": [ "code-8" ],
              "nutriments": {
                "energy-kcal": 123,
                "vitamin-pp": 1,
                "vitamin-pp_unit": "mg"
              }
            }
            """,
            OpenFoodFactsEntry(
                code="44000271",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Apple",
                energy=123.0,
                vitamin_b3=0.001,
            ),
        ),
        (
            """
            { "id": "8",
              "code": "44000271",
              "created_t": 1234567890,
              "product_name": "Apple",
              "codes_tags": [ "code-8" ],
              "nutriments": {
                "energy-kcal": 123,
                "vitamin-b3": 1,
                "vitamin-b3_unit": "mg"
              }
            }
            """,
            OpenFoodFactsEntry(
                code="44000271",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Apple",
                energy=123.0,
                vitamin_b3=0.001,
            ),
        ),
        (
            """
            { "id": "9",
              "code": "44000271",
              "created_t": 1234567890,
              "product_name": "Apple",
              "codes_tags": [ "code-8" ],
              "nutriments": {
                "energy-kcal": 123,
                "vitamin-pp": 1,
                "vitamin-pp_unit": "mg",
                "vitamin-b3": 1,
                "vitamin-b3_unit": "mg"
              }
            }
            """,
            OpenFoodFactsEntry(
                code="44000271",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Apple",
                energy=123.0,
                vitamin_b3=0.002,
            ),
        ),
        (
            """
            { "id": "10",
              "code": "44000271",
              "created_t": 1234567890,
              "product_name": "Apple",
              "codes_tags": [ "code-8" ],
              "nutriments": {
                "energy-kcal": 123,
                "folates": 1,
                "folates_unit": "mg"
              }
            }
            """,
            OpenFoodFactsEntry(
                code="44000271",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Apple",
                energy=123.0,
                vitamin_b9=0.001,
            ),
        ),
        (
            """
            { "id": "11",
              "code": "44000271",
              "created_t": 1234567890,
              "product_name": "Apple",
              "codes_tags": [ "code-8" ],
              "nutriments": {
                "energy-kcal": 123,
                "vitamin-b9": 1,
                "vitamin-b9_unit": "mg"
              }
            }
            """,
            OpenFoodFactsEntry(
                code="44000271",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Apple",
                energy=123.0,
                vitamin_b9=0.001,
            ),
        ),
        (
            """
            { "id": "12",
              "code": "44000271",
              "created_t": 1234567890,
              "product_name": "Apple",
              "codes_tags": [ "code-8" ],
              "nutriments": {
                "energy-kcal": 123,
                "folates": 1,
                "folates_unit": "mg",
                "vitamin-b9": 1,
                "vitamin-b9_unit": "mg"
              }
            }
            """,
            OpenFoodFactsEntry(
                code="44000271",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Apple",
                energy=123.0,
                vitamin_b9=0.002,
            ),
        ),
        (
            """
            { "id": "12",
              "code": "44000271",
              "created_t": 1234567890,
              "product_name": "Apple",
              "codes_tags": [ "code-8" ],
              "nutriments": {
                "energy-kcal": 123,
                "folates": 1,
                "folates_unit": "mg",
                "vitamin-b9": 1,
                "vitamin-b9_unit": "mg"
              },
              "brands" : "brand1,    brand2,brand3"
            }
            """,
            OpenFoodFactsEntry(
                code="44000271",
                created=datetime.date.fromtimestamp(1234567890),
                last_updated=datetime.date.fromtimestamp(1234567890),
                name="Apple",
                brands="brand1,brand2,brand3",
                energy=123.0,
                vitamin_b9=0.002,
            ),
        ),
    ],
)
def test_convert_entry(data: str, expected: OpenFoodFactsEntry) -> None:
    result = _convert_entry(data.encode())
    assert repr(result) == repr(expected)


@pytest.mark.parametrize(
    ("value", "unit", "value_100g", "factor", "name", "expected"),
    [
        (
            100.0,
            "g",
            200.0,
            1.0,
            "prefer 100g value",
            200.0,
        ),
        (
            None,
            "g",
            None,
            1.0,
            "No value",
            None,
        ),
        (
            0.0,
            "g",
            None,
            1.0,
            "Convert zero value to None",
            None,
        ),
        (
            1000000.0,
            "µg",
            None,
            2.0,
            "Convert micrograms to grams",
            2.0,
        ),
        (
            1000000.0,
            "μg",
            None,
            3.0,
            "Convert micrograms to grams (different encoding)",
            3.0,
        ),
        (
            1000000.0,
            "&#181;g",
            None,
            4.0,
            "Convert micrograms to grams (garbled HTML encoding)",
            4.0,
        ),
        (
            1000.0,
            "mg",
            None,
            5.0,
            "Convert milligrams to grams (mg)",
            5.0,
        ),
        (
            1000.0,
            "mcg",
            None,
            6.0,
            "Convert milligrams to grams (mcg)",
            6.0,
        ),
        (
            1.0,
            "g",
            None,
            7.0,
            "Convert grams to grams (g)",
            7.0,
        ),
        (
            1.0,
            "g/100mL",
            None,
            8.0,
            "Convert grams to grams (g/100mL)",
            8.0,
        ),
        (
            1.0,
            "g/100g",
            None,
            9.0,
            "Convert grams to grams (g/100g)",
            9.0,
        ),
        (
            1.0,
            "IU",
            None,
            1.0,
            "vitamin_a",
            0.3 / 1000000,
        ),
        (
            40.0,
            "IU",
            None,
            1.0,
            "vitamin_d",
            1 / 1000000,
        ),
        (
            1.0,
            "IU",
            None,
            1.0,
            "vitamin_e",
            0.67 / 1000000,
        ),
        (
            1.0,
            "IU",
            None,
            1.0,
            "calcium",
            None,
        ),
        (
            1.0,
            "IU",
            None,
            1.0,
            "vitamin_c",
            None,
        ),
        (
            1.0,
            "IU",
            None,
            1.0,
            "iron",
            None,
        ),
        (
            1.0,
            "IU",
            None,
            1.0,
            "random junk",
            None,
        ),
        (
            1.0,
            "garbage unit",
            None,
            1.0,
            "only grams (+ fractions) and IU is supported / required",
            None,
        ),
    ],
)
def test_convert_nutrient(
    value: Optional[float],
    unit: str,
    value_100g: Optional[float],
    factor: float,
    name: str,
    expected: Optional[float],
) -> None:
    assert (
        _convert_nutrient(
            value=value,
            unit=unit,
            value_100g=value_100g,
            factor=factor,
            name=name,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("code", "valid"),
    [
        (0, True),
        (1, True),
        (19, True),
        (20, False),
        (29, False),
        (30, True),
        (39, True),
        (40, False),
        (49, False),
        (50, True),
        (59, True),
        (60, True),
        (99, True),
        (100, True),
        (139, True),
        (200, False),
        (299, False),
        (300, True),
        (380, True),
        (959, False),
    ],
)
def test_valid_ean_country_code(code: int, valid: bool) -> None:  # noqa: FBT001
    assert _valid_ean_country_code(code) == valid


@pytest.mark.parametrize(
    ("code", "valid"),
    [
        ("", False),
        ("000000000", False),
        ("no-digit", False),
        ("96000000", False),
        ("44000272", False),
        ("44000271", True),
    ],
)
def test_valid_ean8(code: str, valid: bool) -> None:  # noqa: FBT001
    assert _valid_ean8(code) == valid


@pytest.mark.parametrize(
    ("code", "valid"),
    [
        ("", False),
        ("n0t-just-dgts", False),
        ("00000000", False),
        ("0000000000000", False),
        ("0000000999999", False),
        ("0000001000000", False),
        ("0000999999999", False),
        ("0211000000000", False),
        ("4017100290007", False),
        ("4017100290008", True),
    ],
)
def test_valid_ean13(code: str, valid: bool) -> None:  # noqa: FBT001
    assert _valid_ean13(code) == valid


@pytest.mark.parametrize(
    "data",
    [
        ["entry1", "invalid", "entry2"],
        200 * ["entry"],
        100 * ["entry_a"] + ["invalid"] + 100 * ["entry_b"],
    ],
)
def test_import_url(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, data: list[str]) -> None:

    saved_url: Optional[str] = None
    saved_chunk_size: Optional[int] = None

    def dummy_download(
        url: str, chunk_size: int = 8192
    ) -> Generator[tuple[bytes, Optional[float]], None, None]:
        nonlocal saved_url, saved_chunk_size
        saved_url = url
        saved_chunk_size = chunk_size

        position = 0
        compressed = gzip.compress("\n".join(data).encode())
        while position < len(compressed):
            yield (compressed[position : position + chunk_size], None)
            position += chunk_size

    def dummy_convert_entry(line: bytes) -> OpenFoodFactsEntry:
        if line == b"invalid":
            raise openfoodfacts.InvalidDataError

        return OpenFoodFactsEntry(
            code=line.decode().upper(),
            name="dummy",
            created=datetime.date.today(),
            last_updated=datetime.date.today(),
        )

    with monkeypatch.context() as m, app.app_context():
        app.config["DATABASE"] = f"sqlite:///{tmp_path / 'db.sqlite'}"
        app.config["OPENFOODFACTS_JSONL_URL"] = "http://example.com"
        app.config["OPENFOODFACTS_DELTA_URL"] = "http://example.com"
        m.setattr(openfoodfacts, "_convert_entry", dummy_convert_entry)
        m.setattr(openfoodfacts.utils, "download", dummy_download)  # type: ignore[attr-defined]
        import_url(url="dummy-url", chunk_size=1234, commit_interval=100)
        assert [
            e.code
            for e in (
                database.session.get(OpenFoodFactsEntry, d.upper()) for d in data if d != "invalid"
            )
            if e is not None
        ] == [d.upper() for d in data if d != "invalid"]

    assert saved_url == "dummy-url"
    assert saved_chunk_size == 1234