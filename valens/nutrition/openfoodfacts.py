#!/usr/bin/env python3

import datetime
import json
import sys
import zlib
from collections.abc import Generator
from typing import Optional

import requests
from pydantic import BaseModel, Field

from valens import app, database, models

ETHANOL_DENSITY_G_PER_ML = 0.789

REGULAR_NUTRIMENT_NAMES = [
    "bicarbonate",
    "caffeine",
    "calcium",
    "carbohydrates",
    "chloride",
    "cholesterol",
    "chromium",
    "copper",
    "fat",
    "fiber",
    "fluoride",
    "iodine",
    "iron",
    "lactose",
    "magnesium",
    "manganese",
    "molybdenum",
    "monounsaturated_fat",
    "omega_3_fat",
    "omega_6_fat",
    "phosphorus",
    "polyunsaturated_fat",
    "potassium",
    "proteins",
    "salt",
    "saturated_fat",
    "selenium",
    "sodium",
    "starch",
    "sugars",
    "taurine",
    "trans_fat",
    "vitamin_a",
    "vitamin_b12",
    "vitamin_b1",
    "vitamin_b2",
    "vitamin_b5",
    "vitamin_b6",
    "vitamin_b7",
    "vitamin_c",
    "vitamin_d",
    "vitamin_e",
    "vitamin_k",
    "vitamin_k1",
    "zinc",
]

LANGUAGES = [
    "ar",
    "bg",
    "ca",
    "ch",
    "cs",
    "da",
    "de",
    "el",
    "en",
    "es",
    "et",
    "fi",
    "fr",
    "he",
    "hr",
    "hu",
    "id",
    "it",
    "ja",
    "la",
    "lc",
    "lt",
    "lv",
    "nb",
    "nl",
    "no",
    "pl",
    "pt",
    "ro",
    "ru",
    "sk",
    "sl",
    "sr",
    "sv",
    "th",
    "tr",
    "uk",
    "vi",
    "zh",
]

UNITS = {
    None: models.Unit.G,
    "g": models.Unit.G,
    "ml": models.Unit.ML,
}


class InvalidDataError(Exception):
    pass


class OpenFoodFactsNutriments(BaseModel):
    """Model for importing OpenFoodFacts "nutriments" sub-entry from JSONL dump."""

    alcohol: Optional[float] = Field(default=None)
    alcohol_unit: Optional[str] = Field(default=None)
    alcohol_100g: Optional[float] = Field(default=None)

    bicarbonate: Optional[float] = Field(default=None)
    bicarbonate_unit: Optional[str] = Field(default=None)
    bicarbonate_100g: Optional[float] = Field(default=None)

    caffeine: Optional[float] = Field(default=None)
    caffeine_unit: Optional[str] = Field(default=None)
    caffeine_100g: Optional[float] = Field(default=None)

    calcium: Optional[float] = Field(default=None)
    calcium_unit: Optional[str] = Field(default=None)
    calcium_100g: Optional[float] = Field(default=None)

    carbohydrates: Optional[float] = Field(default=None)
    carbohydrates_unit: Optional[str] = Field(default=None)
    carbohydrates_100g: Optional[float] = Field(default=None)

    chloride: Optional[float] = Field(default=None)
    chloride_unit: Optional[str] = Field(default=None)
    chloride_100g: Optional[float] = Field(default=None)

    cholesterol: Optional[float] = Field(default=None)
    cholesterol_unit: Optional[str] = Field(default=None)
    cholesterol_100g: Optional[float] = Field(default=None)

    chromium: Optional[float] = Field(default=None)
    chromium_unit: Optional[str] = Field(default=None)
    chromium_100g: Optional[float] = Field(default=None)

    copper: Optional[float] = Field(default=None)
    copper_unit: Optional[str] = Field(default=None)
    copper_100g: Optional[float] = Field(default=None)

    energy_kcal: Optional[float] = Field(default=None, alias="energy-kcal")
    energy_kj: Optional[float] = Field(default=None, alias="energy-kj")
    energy_100g: Optional[float] = Field(default=None)

    fat: Optional[float] = Field(default=None)
    fat_unit: Optional[str] = Field(default=None)
    fat_100g: Optional[float] = Field(default=None)

    fiber: Optional[float] = Field(default=None)
    fiber_unit: Optional[str] = Field(default=None)
    fiber_100g: Optional[float] = Field(default=None)

    fluoride: Optional[float] = Field(default=None)
    fluoride_unit: Optional[str] = Field(default=None)
    fluoride_100g: Optional[float] = Field(default=None)

    iodine: Optional[float] = Field(default=None)
    iodine_unit: Optional[str] = Field(default=None)
    iodine_100g: Optional[float] = Field(default=None)

    iron: Optional[float] = Field(default=None)
    iron_unit: Optional[str] = Field(default=None)
    iron_100g: Optional[float] = Field(default=None)

    lactose: Optional[float] = Field(default=None)
    lactose_unit: Optional[str] = Field(default=None)
    lactose_100g: Optional[float] = Field(default=None)

    magnesium: Optional[float] = Field(default=None)
    magnesium_unit: Optional[str] = Field(default=None)
    magnesium_100g: Optional[float] = Field(default=None)

    manganese: Optional[float] = Field(default=None)
    manganese_unit: Optional[str] = Field(default=None)
    manganese_100g: Optional[float] = Field(default=None)

    molybdenum: Optional[float] = Field(default=None)
    molybdenum_unit: Optional[str] = Field(default=None)
    molybdenum_100g: Optional[float] = Field(default=None)

    monounsaturated_fat: Optional[float] = Field(default=None, alias="monounsaturated-fat")
    monounsaturated_fat_unit: Optional[str] = Field(default=None, alias="monounsaturated-fat_unit")
    monounsaturated_fat_100g: Optional[float] = Field(
        default=None, alias="monounsaturated-fat_100g"
    )

    omega_3_fat: Optional[float] = Field(default=None, alias="omega-3-fat")
    omega_3_fat_unit: Optional[str] = Field(default=None, alias="omega-3-fat_unit")
    omega_3_fat_100g: Optional[float] = Field(default=None, alias="omega-3-fat_100g")

    omega_6_fat: Optional[float] = Field(default=None, alias="omega-6-fat")
    omega_6_fat_unit: Optional[str] = Field(default=None, alias="omega-6-fat_unit")
    omega_6_fat_100g: Optional[float] = Field(default=None, alias="omega-6-fat_100g")

    phosphorus: Optional[float] = Field(default=None)
    phosphorus_unit: Optional[str] = Field(default=None)
    phosphorus_100g: Optional[float] = Field(default=None)

    polyunsaturated_fat: Optional[float] = Field(default=None, alias="polyunsaturated-fat")
    polyunsaturated_fat_unit: Optional[str] = Field(default=None, alias="polyunsaturated-fat_unit")
    polyunsaturated_fat_100g: Optional[float] = Field(
        default=None, alias="polyunsaturated-fat_100g"
    )

    potassium: Optional[float] = Field(default=None)
    potassium_unit: Optional[str] = Field(default=None)
    potassium_100g: Optional[float] = Field(default=None)

    proteins: Optional[float] = Field(default=None)
    proteins_unit: Optional[str] = Field(default=None)
    proteins_100g: Optional[float] = Field(default=None)

    salt: Optional[float] = Field(default=None)
    salt_unit: Optional[str] = Field(default=None)
    salt_100g: Optional[float] = Field(default=None)

    saturated_fat: Optional[float] = Field(default=None, alias="saturated-fat")
    saturated_fat_unit: Optional[str] = Field(default=None, alias="saturated-fat_unit")
    saturated_fat_100g: Optional[float] = Field(default=None, alias="saturated-fat_100g")

    selenium: Optional[float] = Field(default=None)
    selenium_unit: Optional[str] = Field(default=None)
    selenium_100g: Optional[float] = Field(default=None)

    sodium: Optional[float] = Field(default=None)
    sodium_unit: Optional[str] = Field(default=None)
    sodium_100g: Optional[float] = Field(default=None)

    starch: Optional[float] = Field(default=None)
    starch_unit: Optional[str] = Field(default=None)
    starch_100g: Optional[float] = Field(default=None)

    sugars: Optional[float] = Field(default=None)
    sugars_unit: Optional[str] = Field(default=None)
    sugars_100g: Optional[float] = Field(default=None)

    taurine: Optional[float] = Field(default=None)
    taurine_unit: Optional[str] = Field(default=None)
    taurine_100g: Optional[float] = Field(default=None)

    trans_fat: Optional[float] = Field(default=None, alias="trans-fat")
    trans_fat_unit: Optional[str] = Field(default=None, alias="trans-fat_unit")
    trans_fat_100g: Optional[float] = Field(default=None, alias="trans-fat_100g")

    vitamin_a: Optional[float] = Field(default=None, alias="vitamin-a")
    vitamin_a_unit: Optional[str] = Field(default=None, alias="vitamin-a_unit")
    vitamin_a_100g: Optional[float] = Field(default=None, alias="vitamin-a_100g")

    vitamin_b12: Optional[float] = Field(default=None, alias="vitamin-b12")
    vitamin_b12_unit: Optional[str] = Field(default=None, alias="vitamin-b12_unit")
    vitamin_b12_100g: Optional[float] = Field(default=None, alias="vitamin-b12_100g")

    vitamin_b1: Optional[float] = Field(default=None, alias="vitamin-b1")
    vitamin_b1_unit: Optional[str] = Field(default=None, alias="vitamin-b1_unit")
    vitamin_b1_100g: Optional[float] = Field(default=None, alias="vitamin-b1_100g")

    vitamin_b2: Optional[float] = Field(default=None, alias="vitamin-b2")
    vitamin_b2_unit: Optional[str] = Field(default=None, alias="vitamin-b2_unit")
    vitamin_b2_100g: Optional[float] = Field(default=None, alias="vitamin-b2_100g")

    vitamin_b3: Optional[float] = Field(default=None, alias="vitamin-b3")
    vitamin_b3_unit: Optional[str] = Field(default=None, alias="vitamin-b3_unit")
    vitamin_b3_100g: Optional[float] = Field(default=None, alias="vitamin-b3_100g")

    vitamin_pp: Optional[float] = Field(default=None, alias="vitamin-pp")
    vitamin_pp_unit: Optional[str] = Field(default=None, alias="vitamin-pp_unit")
    vitamin_pp_100g: Optional[float] = Field(default=None, alias="vitamin-pp_100g")

    vitamin_b5: Optional[float] = Field(default=None, alias="pantothenic-acid")
    vitamin_b5_unit: Optional[str] = Field(default=None, alias="pantothenic-acid_unit")
    vitamin_b5_100g: Optional[float] = Field(default=None, alias="pantothenic-acid_100g")

    vitamin_b6: Optional[float] = Field(default=None, alias="vitamin-b6")
    vitamin_b6_unit: Optional[str] = Field(default=None, alias="vitamin-b6_unit")
    vitamin_b6_100g: Optional[float] = Field(default=None, alias="vitamin-b6_100g")

    vitamin_b7: Optional[float] = Field(default=None, alias="biotin")
    vitamin_b7_unit: Optional[str] = Field(default=None, alias="biotin-b7_unit")
    vitamin_b7_100g: Optional[float] = Field(default=None, alias="biotin-b7_100g")

    vitamin_b9: Optional[float] = Field(default=None, alias="vitamin-b9")
    vitamin_b9_unit: Optional[str] = Field(default=None, alias="vitamin-b9_unit")
    vitamin_b9_100g: Optional[float] = Field(default=None, alias="vitamin-b9_100g")

    # Synonymous to Vitamin B9:
    folates: Optional[float] = Field(default=None, alias="folates")
    folates_unit: Optional[str] = Field(default=None, alias="folates_unit")
    folates_100g: Optional[float] = Field(default=None, alias="folates_100g")

    vitamin_c: Optional[float] = Field(default=None, alias="vitamin-c")
    vitamin_c_unit: Optional[str] = Field(default=None, alias="vitamin-c_unit")
    vitamin_c_100g: Optional[float] = Field(default=None, alias="vitamin-c_100g")

    vitamin_d: Optional[float] = Field(default=None, alias="vitamin-d")
    vitamin_d_unit: Optional[str] = Field(default=None, alias="vitamin-d_unit")
    vitamin_d_100g: Optional[float] = Field(default=None, alias="vitamin-d_100g")

    vitamin_e: Optional[float] = Field(default=None, alias="vitamin-e")
    vitamin_e_unit: Optional[str] = Field(default=None, alias="vitamin-e_unit")
    vitamin_e_100g: Optional[float] = Field(default=None, alias="vitamin-e_100g")

    vitamin_k: Optional[float] = Field(default=None, alias="vitamin-k")
    vitamin_k_unit: Optional[str] = Field(default=None, alias="vitamin-k_unit")
    vitamin_k_100g: Optional[float] = Field(default=None, alias="vitamin-k_100g")

    vitamin_k1: Optional[float] = Field(default=None, alias="phylloquinone")
    vitamin_k1_unit: Optional[str] = Field(default=None, alias="phylloquinone_unit")
    vitamin_k1_100g: Optional[float] = Field(default=None, alias="phylloquinone_100g")

    zinc: Optional[float] = Field(default=None, alias="zinc")
    zinc_unit: Optional[str] = Field(default=None, alias="zinc_unit")
    zinc_100g: Optional[float] = Field(default=None, alias="zinc_100g")


class OpenFoodFactsEntry(BaseModel):
    """Model for importing OpenFoodFacts entry from JSONL dump."""

    identifier: Optional[str] = Field(default=None, alias="id")
    code: str
    created_t: Optional[int] = Field(default=None)

    product_name: Optional[str] = Field(default=None)
    product_name_ar: Optional[str] = Field(default=None)
    product_name_bg: Optional[str] = Field(default=None)
    product_name_ca: Optional[str] = Field(default=None)
    product_name_ch: Optional[str] = Field(default=None)
    product_name_cs: Optional[str] = Field(default=None)
    product_name_da: Optional[str] = Field(default=None)
    product_name_de: Optional[str] = Field(default=None)
    product_name_el: Optional[str] = Field(default=None)
    product_name_en: Optional[str] = Field(default=None)
    product_name_es: Optional[str] = Field(default=None)
    product_name_et: Optional[str] = Field(default=None)
    product_name_fi: Optional[str] = Field(default=None)
    product_name_fr: Optional[str] = Field(default=None)
    product_name_he: Optional[str] = Field(default=None)
    product_name_hr: Optional[str] = Field(default=None)
    product_name_hu: Optional[str] = Field(default=None)
    product_name_id: Optional[str] = Field(default=None)
    product_name_it: Optional[str] = Field(default=None)
    product_name_ja: Optional[str] = Field(default=None)
    product_name_la: Optional[str] = Field(default=None)
    product_name_lc: Optional[str] = Field(default=None)
    product_name_lt: Optional[str] = Field(default=None)
    product_name_lv: Optional[str] = Field(default=None)
    product_name_nb: Optional[str] = Field(default=None)
    product_name_nl: Optional[str] = Field(default=None)
    product_name_no: Optional[str] = Field(default=None)
    product_name_pl: Optional[str] = Field(default=None)
    product_name_pt: Optional[str] = Field(default=None)
    product_name_ro: Optional[str] = Field(default=None)
    product_name_ru: Optional[str] = Field(default=None)
    product_name_sk: Optional[str] = Field(default=None)
    product_name_sl: Optional[str] = Field(default=None)
    product_name_sr: Optional[str] = Field(default=None)
    product_name_sv: Optional[str] = Field(default=None)
    product_name_th: Optional[str] = Field(default=None)
    product_name_tr: Optional[str] = Field(default=None)
    product_name_uk: Optional[str] = Field(default=None)
    product_name_vi: Optional[str] = Field(default=None)
    product_name_zh: Optional[str] = Field(default=None)

    product_quantity: Optional[float] = Field(default=None)
    product_quantity_unit: Optional[str] = Field(default=None)

    no_nutrition_data: Optional[str] = Field(default=None)

    serving_quantity: Optional[float] = Field(default=None)
    serving_quantity_unit: Optional[str] = Field(default=None)

    nutrition_data_per: Optional[str] = Field(default=None)

    nutriments: Optional[OpenFoodFactsNutriments] = Field(default=None, alias="nutriments")
    nutriments_estimated: Optional[OpenFoodFactsNutriments] = Field(
        default=None, alias="nutriments_estimated"
    )

    last_updated_t: Optional[int] = Field(default=None)
    brands: Optional[str] = Field(default=None)
    codes_tags: Optional[list[str]] = Field(default=None)

    obsolete: Optional[str] = Field(default=None)


def _convert_nutrient(  # noqa: C901
    value: Optional[float],
    unit: Optional[str],
    value_100g: Optional[float],
    factor: float,
    name: str,
) -> Optional[float]:

    if value_100g:
        return value_100g

    if value is None or unit is None:
        return None

    if value == 0.0:
        return None

    if unit in ["µg", "μg", "&#181;g"]:
        return factor * value / 1000000.0

    if unit in ["mg", "mcg"]:
        return factor * value / 1000.0

    if unit in ["g", "g/100mL", "g/100g", ""]:
        return factor * value

    if unit == "IU":
        if name == "vitamin_a":
            # Source: https://ods.od.nih.gov/factsheets/VitaminA-HealthProfessional/
            return factor * value * 0.0000003
        if name == "vitamin_d":
            # Source: https://ods.od.nih.gov/factsheets/VitaminD-HealthProfessional/
            return factor * value * 0.000000025
        if name == "vitamin_e":
            # Source: https://ods.od.nih.gov/factsheets/VitaminE-HealthProfessional/
            return factor * value * 0.00000067

    return None


def _valid_ean_country_code(code: int) -> bool:
    """
    Check whether `code` is a valid country prefix for use in product bar codes.

    Source: https://www.gs1.org/standards/id-keys/company-prefix
    """

    # Restricted Circulation Numbers
    if 20 <= code < 30:
        return False

    # Restricted Circulation Numbers
    if 40 <= code < 50:
        return False

    # Restricted Circulation Numbers
    if 200 <= code < 300:
        return False

    # Other restricted uses
    if code > 958:
        return False

    return True


def _valid_ean8(ean: str) -> bool:
    if len(ean) != 8 or not ean.isdigit():
        return False

    if not _valid_ean_country_code(int(ean[0:3])):
        return False

    return (
        sum(int(ean[i]) for i in range(1, 8, 2)) + 3 * sum(int(ean[i]) for i in range(0, 7, 2))
    ) % 10 == 0


def _valid_ean13(ean: str) -> bool:
    """
    Check whether `ean` is a valid EAN-13 code.

    Source: https://www.gs1.org/standards/id-keys/company-prefix
    """

    if len(ean) != 13 or not ean.isdigit():
        return False

    if int(ean) < 1000000:
        # Restricted Circulation Numbers
        return False

    if 1000000 <= int(ean) < 100000000:
        # Unused to avoid EAN-8 collision
        return False

    if not _valid_ean_country_code(int(ean[0:3])):
        return False

    return (
        sum(int(ean[2 * i]) + 3 * int(ean[2 * i + 1]) for i in range(6)) + int(ean[-1])
    ) % 10 == 0


def _convert_entry(  # noqa: PLR0912, C901, PLR0915
    line: bytes,
) -> models.OpenFoodFactsEntry:

    try:
        entry = OpenFoodFactsEntry(**json.loads(line.decode()))
    except json.JSONDecodeError as e:
        raise InvalidDataError(str(e)) from e

    if entry.no_nutrition_data and entry.no_nutrition_data.lower() in ["on", "true"]:
        raise InvalidDataError("no nutrition data")

    if entry.identifier is None:
        raise InvalidDataError("no identifier")

    if int(entry.identifier) == 0:
        raise InvalidDataError(f"invalid identifier ({entry.identifier})")

    if entry.created_t is None:
        raise InvalidDataError("no creation date")

    if entry.product_name is None:
        raise InvalidDataError("no product name")

    if not entry.codes_tags:
        raise InvalidDataError("no codes tags")

    if entry.obsolete and entry.obsolete.lower() == "on":
        raise InvalidDataError("obsolete entry")

    # Validate EAN code
    code: Optional[str] = None

    if "code-8" in entry.codes_tags:
        code = entry.code.zfill(8)
        if not _valid_ean8(code):
            raise InvalidDataError("invalid EAN-8 code")
    elif "code-13" in entry.codes_tags:
        code = entry.code.zfill(13)
        if not _valid_ean13(code):
            raise InvalidDataError("invalid EAN-13 code")
    else:
        raise InvalidDataError("no supported code tag found")

    nutriments = entry.nutriments or entry.nutriments_estimated

    if nutriments is None:
        raise InvalidDataError("no nutriments present")

    created = datetime.date.fromtimestamp(entry.created_t)

    quantity: Optional[float] = entry.product_quantity or None
    unit: Optional[models.Unit] = UNITS.get(entry.product_quantity_unit, models.Unit.G)

    serving_quantity: Optional[float] = None
    if entry.serving_quantity:
        if entry.serving_quantity_unit in ["g", None]:
            serving_quantity = entry.serving_quantity
        elif entry.serving_quantity_unit in ["%"]:
            if quantity is None:
                raise InvalidDataError("serving_quantity in percent, but no product_quantity")
            serving_quantity = entry.serving_quantity / 100.0 * quantity
        else:
            raise InvalidDataError(
                f"unsupported serving quantity unit: {entry.serving_quantity_unit}"
            )

    factor = 1.0

    # Adjust factor if nutrition data is per serving
    if entry.nutrition_data_per and entry.nutrition_data_per == "serving":
        if serving_quantity:
            factor *= 100.0 / serving_quantity
        else:
            raise InvalidDataError("nutrition data per serving, but no serving quantity")

    # Alcohol
    alcohol: Optional[float] = None
    if nutriments.alcohol is not None:
        if nutriments.alcohol_unit is None:
            raise InvalidDataError("alcohol has no unit")
        if nutriments.alcohol_unit in ["% vol", "% vol / *", "vol", "%"]:
            alcohol = factor * nutriments.alcohol * ETHANOL_DENSITY_G_PER_ML
        elif nutriments.alcohol_unit == "g":
            alcohol = factor * nutriments.alcohol
        else:
            raise InvalidDataError(f"invalid alcohol unit: {nutriments.alcohol_unit}")

    # Energy
    energy: Optional[float] = None
    if nutriments.energy_kcal is not None:
        energy = factor * nutriments.energy_kcal
    elif nutriments.energy_kj is not None:
        energy = factor * nutriments.energy_kj * 0.23900574

    # Vitamin B3
    vitamin_b3: Optional[float] = _convert_nutrient(
        value=nutriments.vitamin_b3,
        unit=nutriments.vitamin_b3_unit,
        value_100g=nutriments.vitamin_b3_100g,
        factor=factor,
        name="vitamin_b3",
    )

    vitamin_pp: Optional[float] = _convert_nutrient(
        value=nutriments.vitamin_pp,
        unit=nutriments.vitamin_pp_unit,
        value_100g=nutriments.vitamin_pp_100g,
        factor=factor,
        name="vitamin_pp",
    )

    if vitamin_b3 is not None:
        if vitamin_pp is not None:
            vitamin_b3 += vitamin_pp
    elif vitamin_pp is not None:
        vitamin_b3 = vitamin_pp

    # Vitamin B9
    vitamin_b9: Optional[float] = _convert_nutrient(
        value=nutriments.vitamin_b9,
        unit=nutriments.vitamin_b9_unit,
        value_100g=nutriments.vitamin_b9_100g,
        factor=factor,
        name="vitamin_b9",
    )

    folates: Optional[float] = _convert_nutrient(
        value=nutriments.folates,
        unit=nutriments.folates_unit,
        value_100g=nutriments.folates_100g,
        factor=factor,
        name="folates",
    )

    if vitamin_b9 is not None:
        if folates is not None:
            vitamin_b9 += folates
    elif folates is not None:
        vitamin_b9 = folates

    localized_names = ",".join(
        f"{language}:{getattr(entry, f'product_name_{language}')}"
        for language in LANGUAGES
        if (
            getattr(entry, f"product_name_{language}")
            and getattr(entry, f"product_name_{language}") != entry.product_name
        )
    )

    nutriment_values = {
        "alcohol": alcohol or None,
        "energy": energy or None,
        "vitamin_b3": vitamin_b3 or None,
        "vitamin_b9": vitamin_b9 or None,
        **{
            name: _convert_nutrient(
                value=getattr(nutriments, name),
                unit=getattr(nutriments, f"{name}_unit"),
                value_100g=getattr(nutriments, f"{name}_100g"),
                factor=factor,
                name=name,
            )
            or None
            for name in REGULAR_NUTRIMENT_NAMES
        },
    }

    if all(v is None for v in nutriment_values.values()):
        raise InvalidDataError("all nutrition data is zero")

    metadata = {
        "localized_names": localized_names or None,
        "brands": (
            ",".join(brand.strip() for brand in entry.brands.split(",")) if entry.brands else None
        ),
        "serving_quantity": serving_quantity,
        "quantity": quantity,
        "unit": unit,
    }

    return models.OpenFoodFactsEntry(
        code=code,
        created=created,
        last_updated=(
            datetime.date.fromtimestamp(entry.last_updated_t) if entry.last_updated_t else created
        ),
        name=entry.product_name,
        **{k: v for k, v in metadata.items() if v},
        **{k: v for k, v in nutriment_values.items() if v},
    )


def _download_chunked(
    url: str, chunk_size: int = 8192
) -> Generator[tuple[bytes, Optional[float]], None, None]:
    partial_input: bytes = b""
    partial_output: Optional[bytes] = None

    bytes_read: int = 0

    r = requests.get(url, stream=True)
    r.raise_for_status()

    content_length = int(r.headers["Content-Length"]) if "Content-Length" in r.headers else None
    decomp = zlib.decompressobj(32 + zlib.MAX_WBITS)
    progress: Optional[float] = None

    for chunk in r.iter_content(chunk_size=chunk_size):
        if not chunk:
            continue

        bytes_read += len(chunk)
        progress = bytes_read / content_length if content_length else None

        # 42 seems to be the minimum input block size
        if len(partial_input) + len(chunk) < 42:
            partial_input = partial_input + chunk
            continue

        result = decomp.decompress(partial_input + chunk)
        partial_input = b""

        lines = result.split(b"\n")
        if len(lines) > 1:
            yield lines[0] if partial_output is None else partial_output + lines[0], progress
            partial_output = None
        partial_output = lines[-1] if partial_output is None else partial_output + lines[-1]
        for line in lines[1:-1]:
            yield line, progress

    if len(partial_input) > 0:
        lines = decomp.decompress(partial_input).split(b"\n")
        if len(lines) > 1:
            yield lines[0] if partial_output is None else partial_output + lines[0], progress
            partial_output = None
        partial_output = lines[-1] if partial_output is None else partial_output + lines[-1]
        for line in lines[1:-1]:
            yield line, progress

    assert partial_output is not None
    yield partial_output, progress


def import_url(url: str) -> None:
    total = 0
    valid = 0
    entries = []
    with app.app_context():
        for line, _ in _download_chunked(url):
            total += 1
            try:
                entry = _convert_entry(line)
            except InvalidDataError:
                continue

            valid += 1
            entries.append(entry)

            if valid % 10000 == 0:
                database.session.add_all(entries)
                database.session.commit()
                entries = []

        database.session.add_all(entries)
        database.session.commit()


if __name__ == "__main__":
    import_url(sys.argv[1])  # pragma: no cover
