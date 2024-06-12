#!/usr/bin/env python3

# FIXME:
#  - validate


import datetime
import enum
import gzip
import json
import multiprocessing
from pathlib import Path
from typing import Optional

import tqdm
from pydantic import BaseModel, Field

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


class CodeType(enum.StrEnum):
    EAN_8 = "ean-8"
    EAN_13 = "ean-13"


class Unit(enum.StrEnum):
    GRAMS = "g"
    MILLILITERS = "ml"


class OpenFoodFactsNutriments(BaseModel):

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


class OpenFoodFactsOutputEntry(BaseModel):

    identifier: int = Field(default=None, alias="identifier")
    code: str
    code_type: CodeType
    created: datetime.date
    last_updated: datetime.date
    name: str

    # FIXME: remove
    entry: int
    factor: float

    quantity: Optional[float] = Field(default=None)
    unit: Optional[str] = Field(default=None)
    serving_quantity: Optional[float] = Field(default=None)

    localized_names: Optional[dict[str, str]] = Field()
    brands: Optional[list[str]] = Field()

    alcohol: Optional[float] = Field(default=None)
    bicarbonate: Optional[float] = Field(default=None)
    caffeine: Optional[float] = Field(default=None)
    calcium: Optional[float] = Field(default=None)
    carbohydrates: Optional[float] = Field(default=None)
    chloride: Optional[float] = Field(default=None)
    cholesterol: Optional[float] = Field(default=None)
    chromium: Optional[float] = Field(default=None)
    copper: Optional[float] = Field(default=None)
    energy: Optional[float] = Field(default=None)
    fat: Optional[float] = Field(default=None)
    fiber: Optional[float] = Field(default=None)
    fluoride: Optional[float] = Field(default=None)
    iodine: Optional[float] = Field(default=None)
    iron: Optional[float] = Field(default=None)
    lactose: Optional[float] = Field(default=None)
    magnesium: Optional[float] = Field(default=None)
    manganese: Optional[float] = Field(default=None)
    molybdenum: Optional[float] = Field(default=None)
    monounsaturated_fat: Optional[float] = Field(default=None)
    omega_3_fat: Optional[float] = Field(default=None)
    omega_6_fat: Optional[float] = Field(default=None)
    phosphorus: Optional[float] = Field(default=None)
    polyunsaturated_fat: Optional[float] = Field(default=None)
    potassium: Optional[float] = Field(default=None)
    proteins: Optional[float] = Field(default=None)
    salt: Optional[float] = Field(default=None)
    saturated_fat: Optional[float] = Field(default=None)
    selenium: Optional[float] = Field(default=None)
    sodium: Optional[float] = Field(default=None)
    starch: Optional[float] = Field(default=None)
    sugars: Optional[float] = Field(default=None)
    taurine: Optional[float] = Field(default=None)
    trans_fat: Optional[float] = Field(default=None)
    vitamin_a: Optional[float] = Field(default=None)
    vitamin_b12: Optional[float] = Field(default=None)
    vitamin_b1: Optional[float] = Field(default=None)
    vitamin_b2: Optional[float] = Field(default=None)
    vitamin_b3: Optional[float] = Field(default=None)
    vitamin_b5: Optional[float] = Field(default=None)
    vitamin_b6: Optional[float] = Field(default=None)
    vitamin_b7: Optional[float] = Field(default=None)
    vitamin_b9: Optional[float] = Field(default=None)
    vitamin_c: Optional[float] = Field(default=None)
    vitamin_d: Optional[float] = Field(default=None)
    vitamin_e: Optional[float] = Field(default=None)
    vitamin_k: Optional[float] = Field(default=None)
    vitamin_k1: Optional[float] = Field(default=None)
    zinc: Optional[float] = Field(default=None)


def convert_nutrient(  # noqa: C901, PLR0911, PLR0912
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
            return factor * value / 300000.0
        if name == "vitamin_d":
            # Source: https://ods.od.nih.gov/factsheets/VitaminD-HealthProfessional/
            return factor * value / 40000000.0
        if name == "vitamin_e":
            # Source: https://ods.od.nih.gov/factsheets/VitaminE-HealthProfessional/
            return factor * value * 0.00067
        if name in ["calcium", "vitamin_c", "iron"]:
            # Invalid
            return None

        raise NotImplementedError(f"IU for {name}")

    if unit == "ml":
        # FIXME: ml not supported
        return None

    if unit in ["% DV", "%"]:
        return None

    raise NotImplementedError(f"{name} {value=}, {unit=}")


def valid_ean_country_code(code: int) -> bool:

    # Source: https://en.wikipedia.org/wiki/List_of_GS1_country_codes

    if 20 <= code < 30:
        # Restricted Circulation Numbers
        return False

    if 40 <= code < 50:
        # Restricted Circulation Numbers
        return False

    if 200 <= code < 300:
        # Restricted Circulation Numbers
        return False

    if code > 958:
        # Other restricted uses
        return False

    return True


def valid_ean8(ean: str) -> bool:
    if len(ean) != 8 or not ean.isdigit():
        return False

    if not valid_ean_country_code(int(ean[0:3])):
        return False

    return (
        sum(int(ean[i]) for i in range(1, 8, 2)) + 3 * sum(int(ean[i]) for i in range(0, 7, 2))
    ) % 10 == 0


def valid_ean13(ean: str) -> bool:
    if len(ean) != 13 or not ean.isdigit():
        return False

    if int(ean) < 1000000:
        # Restricted Circulation Numbers
        return False

    if 1000000 <= int(ean) < 100000000:
        # Unused to avoid EAN-8 collision
        return False

    if not valid_ean_country_code(int(ean[0:3])):
        return False

    return (
        sum(int(ean[2 * i]) + 3 * int(ean[2 * i + 1]) for i in range(6)) + int(ean[-1])
    ) % 10 == 0


def convert(data: tuple[int, str]) -> Optional[str]:  # noqa: C901, PLR0911, PLR0915, PLR0912

    entry = OpenFoodFactsEntry(**json.loads(data[1]))

    if entry.no_nutrition_data and entry.no_nutrition_data.lower() in ["on", "true"]:
        return None

    if entry.identifier is None:
        return None

    if int(entry.identifier) == 0:
        return None

    if entry.created_t is None:
        return None

    if entry.product_name is None:
        return None

    if not entry.codes_tags:
        return None

    if entry.obsolete and entry.obsolete.lower() == "on":
        return None

    # Validate EAN code
    code: Optional[str] = None
    code_type: Optional[CodeType] = None

    if "code-8" in entry.codes_tags:
        code = entry.code.zfill(8)
        if not valid_ean8(code):
            return None
        code_type = CodeType.EAN_8
    elif "code-13" in entry.codes_tags:
        assert code_type is None
        code = entry.code.zfill(13)
        if not valid_ean13(code):
            return None
        code_type = CodeType.EAN_13

    if code_type is None:
        return None

    assert entry.nutriments is not None or entry.nutriments_estimated is not None

    nutriments = entry.nutriments or entry.nutriments_estimated
    created = datetime.date.fromtimestamp(entry.created_t)

    quantity: Optional[float] = entry.product_quantity or None
    unit: Optional[float] = (
        Unit(entry.product_quantity_unit)
        if entry.product_quantity_unit in ["g", "ml"]
        else Unit("g")
    )

    serving_quantity: Optional[float] = None
    if entry.serving_quantity:
        if entry.serving_quantity_unit in ["g", None]:
            serving_quantity = entry.serving_quantity
        elif entry.serving_quantity_unit in ["%"]:
            serving_quantity = entry.serving_quantity / 100.0 * quantity if quantity else None
        elif entry.serving_quantity_unit in ["ml", "mmol/l"]:
            return None
        else:
            return None

    factor = 1.0

    # Adjust factor if nutrition data is per serving
    if entry.nutrition_data_per and entry.nutrition_data_per == "serving":
        if serving_quantity:
            factor *= 100.0 / entry.serving_quantity
        else:
            return None

    # Alcohol
    alcohol: Optional[float] = None
    if nutriments.alcohol is not None:
        assert nutriments.alcohol_unit is not None
        if nutriments.alcohol_unit in ["% vol", "% vol / *", "vol", "%"]:
            alcohol = factor * nutriments.alcohol * ETHANOL_DENSITY_G_PER_ML
        elif nutriments.alcohol_unit == "g":
            alcohol = factor * nutriments.alcohol
        elif nutriments.alcohol_unit in ["-"]:
            return None
        elif nutriments.alcohol_unit == "":
            if nutriments.alcohol == 0:
                # just leave this a null entry
                pass
            return None
        else:
            raise NotImplementedError(f"{nutriments.alcohol=}, {nutriments.alcohol_unit=}")

    # Energy
    energy: Optional[int] = None
    if nutriments.energy_kcal is not None:
        energy = factor * nutriments.energy_kcal
    elif nutriments.energy_kj is not None:
        energy = factor * nutriments.energy_kj * 0.23900574

    # Vitamin B3
    vitamin_b3: Optional[float] = convert_nutrient(
        value=nutriments.vitamin_b3,
        unit=nutriments.vitamin_b3_unit,
        value_100g=nutriments.vitamin_b3_100g,
        factor=factor,
        name="vitamin_b3",
    )

    vitamin_pp: Optional[float] = convert_nutrient(
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
    vitamin_b9: Optional[float] = convert_nutrient(
        value=nutriments.vitamin_b9,
        unit=nutriments.vitamin_b9_unit,
        value_100g=nutriments.vitamin_b9_100g,
        factor=factor,
        name="vitamin_b9",
    )

    folates: Optional[float] = convert_nutrient(
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

    localized_names = {
        language: getattr(entry, f"product_name_{language}") for language in LANGUAGES
    }

    return OpenFoodFactsOutputEntry(
        identifier=int(entry.identifier),
        code=code,
        code_type=code_type,
        created=created,
        last_updated=(
            datetime.date.fromtimestamp(entry.last_updated_t) if entry.last_updated_t else created
        ),
        name=entry.product_name,
        localized_names=(
            {k: v for k, v in localized_names.items() if v} if localized_names else None
        ),
        brands=[brand.strip() for brand in entry.brands.split(",")] if entry.brands else None,
        alcohol=alcohol or None,
        energy=energy or None,
        vitamin_b3=vitamin_b3 or None,
        vitamin_b9=vitamin_b9 or None,
        quantity=quantity,
        serving_quantity=serving_quantity,
        unit=unit,
        **{
            name: convert_nutrient(
                value=getattr(nutriments, name),
                unit=getattr(nutriments, f"{name}_unit"),
                value_100g=getattr(nutriments, f"{name}_100g"),
                factor=factor,
                name=name,
            )
            or None
            for name in REGULAR_NUTRIMENT_NAMES
        },
        entry=data[0],  # FIXME: remove
        factor=factor,  # FIXME: remove
    ).model_dump_json()


def import_off(file: Path):
    total = 0
    valid = 0
    with multiprocessing.Pool(multiprocessing.cpu_count()) as p, Path("output.json").open("w") as o:
        for n in tqdm.tqdm(
            p.imap(
                func=convert,
                iterable=enumerate(gzip.open(file), 1),
                chunksize=1000 * multiprocessing.cpu_count(),
            )
        ):
            total += 1
            if n is None:
                continue
            valid += 1
            o.write(f"{n}\n")

    print(f"{valid} out of {total} valid ({100 * valid/total:.1f}%)")


if __name__ == "__main__":
    import_off("openfoodfacts-products.jsonl.gz")
