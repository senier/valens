#!/usr/bin/env python3

import datetime
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
    "energy",
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
    "vitamin_b3",
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


class OpenFoodFactsNutriments(BaseModel):

    alcohol: Optional[float] = Field(default=None)
    alcohol_value: Optional[float] = Field(default=None)
    alcohol_unit: Optional[str] = Field(default=None)

    bicarbonate: Optional[float] = Field(default=None)
    bicarbonate_value: Optional[float] = Field(default=None)
    bicarbonate_unit: Optional[str] = Field(default=None)

    caffeine: Optional[float] = Field(default=None)
    caffeine_value: Optional[float] = Field(default=None)
    caffeine_unit: Optional[str] = Field(default=None)

    calcium: Optional[float] = Field(default=None)
    calcium_value: Optional[float] = Field(default=None)
    calcium_unit: Optional[str] = Field(default=None)

    carbohydrates: Optional[float] = Field(default=None)
    carbohydrates_value: Optional[float] = Field(default=None)
    carbohydrates_unit: Optional[str] = Field(default=None)

    chloride: Optional[float] = Field(default=None)
    chloride_value: Optional[float] = Field(default=None)
    chloride_unit: Optional[str] = Field(default=None)

    cholesterol: Optional[float] = Field(default=None)
    cholesterol_value: Optional[float] = Field(default=None)
    cholesterol_unit: Optional[str] = Field(default=None)

    chromium: Optional[float] = Field(default=None)
    chromium_value: Optional[float] = Field(default=None)
    chromium_unit: Optional[str] = Field(default=None)

    copper: Optional[float] = Field(default=None)
    copper_value: Optional[float] = Field(default=None)
    copper_unit: Optional[str] = Field(default=None)

    energy: Optional[float] = Field(default=None)
    energy_value: Optional[float] = Field(default=None)
    energy_unit: Optional[str] = Field(default=None)

    energy_kcal: Optional[float] = Field(default=None, alias="energy-kcal")
    energy_kcal_value: Optional[float] = Field(default=None, alias="energy-kcal_value")
    energy_kcal_unit: Optional[str] = Field(default=None, alias="energy-kcal_unit")

    fat: Optional[float] = Field(default=None)
    fat_value: Optional[float] = Field(default=None)
    fat_unit: Optional[str] = Field(default=None)

    fiber: Optional[float] = Field(default=None)
    fiber_value: Optional[float] = Field(default=None)
    fiber_unit: Optional[str] = Field(default=None)

    fluoride: Optional[float] = Field(default=None)
    fluoride_value: Optional[float] = Field(default=None)
    fluoride_unit: Optional[str] = Field(default=None)

    iodine: Optional[float] = Field(default=None)
    iodine_value: Optional[float] = Field(default=None)
    iodine_unit: Optional[str] = Field(default=None)

    iron: Optional[float] = Field(default=None)
    iron_value: Optional[float] = Field(default=None)
    iron_unit: Optional[str] = Field(default=None)

    lactose: Optional[float] = Field(default=None)
    lactose_value: Optional[float] = Field(default=None)
    lactose_unit: Optional[str] = Field(default=None)

    magnesium: Optional[float] = Field(default=None)
    magnesium_value: Optional[float] = Field(default=None)
    magnesium_unit: Optional[str] = Field(default=None)

    manganese: Optional[float] = Field(default=None)
    manganese_value: Optional[float] = Field(default=None)
    manganese_unit: Optional[str] = Field(default=None)

    molybdenum: Optional[float] = Field(default=None)
    molybdenum_value: Optional[float] = Field(default=None)
    molybdenum_unit: Optional[str] = Field(default=None)

    monounsaturated_fat: Optional[float] = Field(default=None, alias="monounsaturated-fat")
    monounsaturated_fat_value: Optional[float] = Field(
        default=None, alias="monounsaturated-fat_value"
    )
    monounsaturated_fat_unit: Optional[str] = Field(default=None, alias="monounsaturated-fat_unit")

    omega_3_fat: Optional[float] = Field(default=None, alias="omega-3-fat")
    omega_3_fat_value: Optional[float] = Field(default=None, alias="omega-3-fat_value")
    omega_3_fat_unit: Optional[str] = Field(default=None, alias="omega-3-fat_unit")

    omega_6_fat: Optional[float] = Field(default=None, alias="omega-6-fat")
    omega_6_fat_value: Optional[float] = Field(default=None, alias="omega-6-fat_value")
    omega_6_fat_unit: Optional[str] = Field(default=None, alias="omega-6-fat_unit")

    phosphorus: Optional[float] = Field(default=None)
    phosphorus_value: Optional[float] = Field(default=None)
    phosphorus_unit: Optional[str] = Field(default=None)

    polyunsaturated_fat: Optional[float] = Field(default=None, alias="polyunsaturated-fat")
    polyunsaturated_fat_value: Optional[float] = Field(
        default=None, alias="polyunsaturated-fat_value"
    )
    polyunsaturated_fat_unit: Optional[str] = Field(default=None, alias="polyunsaturated-fat_unit")

    potassium: Optional[float] = Field(default=None)
    potassium_value: Optional[float] = Field(default=None)
    potassium_unit: Optional[str] = Field(default=None)

    proteins: Optional[float] = Field(default=None)
    proteins_value: Optional[float] = Field(default=None)
    proteins_unit: Optional[str] = Field(default=None)

    salt: Optional[float] = Field(default=None)
    salt_value: Optional[float] = Field(default=None)
    salt_unit: Optional[str] = Field(default=None)

    saturated_fat: Optional[float] = Field(default=None, alias="saturated-fat")
    saturated_fat_value: Optional[float] = Field(default=None, alias="saturated-fat_value")
    saturated_fat_unit: Optional[str] = Field(default=None, alias="saturated-fat_unit")

    selenium: Optional[float] = Field(default=None)
    selenium_value: Optional[float] = Field(default=None)
    selenium_unit: Optional[str] = Field(default=None)

    sodium: Optional[float] = Field(default=None)
    sodium_value: Optional[float] = Field(default=None)
    sodium_unit: Optional[str] = Field(default=None)

    starch: Optional[float] = Field(default=None)
    starch_value: Optional[float] = Field(default=None)
    starch_unit: Optional[str] = Field(default=None)

    sugars: Optional[float] = Field(default=None)
    sugars_value: Optional[float] = Field(default=None)
    sugars_unit: Optional[str] = Field(default=None)

    taurine: Optional[float] = Field(default=None)
    taurine_value: Optional[float] = Field(default=None)
    taurine_unit: Optional[str] = Field(default=None)

    trans_fat: Optional[float] = Field(default=None, alias="trans-fat")
    trans_fat_value: Optional[float] = Field(default=None, alias="trans-fat_value")
    trans_fat_unit: Optional[str] = Field(default=None, alias="trans-fat_unit")

    vitamin_a: Optional[float] = Field(default=None, alias="vitamin-a")
    vitamin_a_value: Optional[float] = Field(default=None, alias="vitamin-a_value")
    vitamin_a_unit: Optional[str] = Field(default=None, alias="vitamin-a_unit")

    vitamin_b12: Optional[float] = Field(default=None, alias="vitamin-b12")
    vitamin_b12_value: Optional[float] = Field(default=None, alias="vitamin-b12_value")
    vitamin_b12_unit: Optional[str] = Field(default=None, alias="vitamin-b12_unit")

    vitamin_b1: Optional[float] = Field(default=None, alias="vitamin-b1")
    vitamin_b1_value: Optional[float] = Field(default=None, alias="vitamin-b1_value")
    vitamin_b1_unit: Optional[str] = Field(default=None, alias="vitamin-b1_unit")

    vitamin_b2: Optional[float] = Field(default=None, alias="vitamin-b2")
    vitamin_b2_value: Optional[float] = Field(default=None, alias="vitamin-b2_value")
    vitamin_b2_unit: Optional[str] = Field(default=None, alias="vitamin-b2_unit")

    vitamin_b3: Optional[float] = Field(default=None, alias="vitamin-b3")
    vitamin_b3_value: Optional[float] = Field(default=None, alias="vitamin-b3_value")
    vitamin_b3_unit: Optional[str] = Field(default=None, alias="vitamin-b3_unit")

    vitamin_b5: Optional[float] = Field(default=None, alias="pantothenic-acid")
    vitamin_b5_value: Optional[float] = Field(default=None, alias="pantothenic-acid_value")
    vitamin_b5_unit: Optional[str] = Field(default=None, alias="pantothenic-acid_unit")

    vitamin_b6: Optional[float] = Field(default=None, alias="vitamin-b6")
    vitamin_b6_value: Optional[float] = Field(default=None, alias="vitamin-b6_value")
    vitamin_b6_unit: Optional[str] = Field(default=None, alias="vitamin-b6_unit")

    vitamin_b7: Optional[float] = Field(default=None, alias="biotin")
    vitamin_b7_value: Optional[float] = Field(default=None, alias="biotin_value")
    vitamin_b7_unit: Optional[str] = Field(default=None, alias="biotin-b7_unit")

    vitamin_b9: Optional[float] = Field(default=None, alias="vitamin-b9")
    vitamin_b9_value: Optional[float] = Field(default=None, alias="vitamin-b9_value")
    vitamin_b9_unit: Optional[str] = Field(default=None, alias="vitamin-b9_unit")

    # Synonymous to Vitamin B9:
    folates: Optional[float] = Field(default=None, alias="folates")
    folates_value: Optional[float] = Field(default=None, alias="folates_value")
    folates_unit: Optional[str] = Field(default=None, alias="folates_unit")

    vitamin_c: Optional[float] = Field(default=None, alias="vitamin-c")
    vitamin_c_value: Optional[float] = Field(default=None, alias="vitamin-c_value")
    vitamin_c_unit: Optional[str] = Field(default=None, alias="vitamin-c_unit")

    vitamin_d: Optional[float] = Field(default=None, alias="vitamin-d")
    vitamin_d_value: Optional[float] = Field(default=None, alias="vitamin-d_value")
    vitamin_d_unit: Optional[str] = Field(default=None, alias="vitamin-d_unit")

    vitamin_e: Optional[float] = Field(default=None, alias="vitamin-e")
    vitamin_e_value: Optional[float] = Field(default=None, alias="vitamin-e_value")
    vitamin_e_unit: Optional[str] = Field(default=None, alias="vitamin-e_unit")

    vitamin_k: Optional[float] = Field(default=None, alias="vitamin-k")
    vitamin_k_value: Optional[float] = Field(default=None, alias="vitamin-k_value")
    vitamin_k_unit: Optional[str] = Field(default=None, alias="vitamin-k_unit")

    vitamin_k1: Optional[float] = Field(default=None, alias="phylloquinone")
    vitamin_k1_value: Optional[float] = Field(default=None, alias="phylloquinone_value")
    vitamin_k1_unit: Optional[str] = Field(default=None, alias="phylloquinone_unit")

    zinc: Optional[float] = Field(default=None, alias="zinc")
    zinc_value: Optional[float] = Field(default=None, alias="zinc_value")
    zinc_unit: Optional[str] = Field(default=None, alias="zinc_unit")


class OpenFoodFactsOutputEntry(BaseModel):

    identifier: int = Field(default=None, alias="identifier")
    code: int
    created: datetime.date
    last_updated: datetime.date
    name: str

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


class OpenFoodFactsEntry(BaseModel):

    identifier: Optional[str] = Field(default=None, alias="id")
    code: str
    created_t: Optional[int] = Field(default=None)
    product_name: Optional[str] = Field(default=None)

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


def convert_nutrient(  # noqa: C901
    value: Optional[float], unit: Optional[str], factor: float, name: str
) -> Optional[float]:
    if value is None:
        return None

    if unit is None:
        return None

    if value == 0.0:
        return value

    if unit in ["µg", "μg", "&#181;g"]:
        return factor * value / 1000000.0

    if unit in ["mg", "mcg"]:
        return factor * value / 1000.0

    if unit in ["g", "g/100mL", "g/100g"]:
        return factor * value

    if unit in ["kcal", "Cal"]:
        return factor * value

    if unit in ["kJ", "kj", "Kj"]:
        return factor * value * 0.23900574

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

    if unit == "":
        # assume grams per 100 g if values are below 100
        if factor * value <= 100.0:
            return factor * value
        # assume milligrams and convert to grams per 100 g if values are below 1000
        if factor * value <= 10000.0:
            return factor * value / 1000.0

    raise NotImplementedError(f"{name} {value=}, {unit=}")


def convert(line: str) -> Optional[str]:  # noqa: C901

    entry = OpenFoodFactsEntry(**json.loads(line))

    if entry.no_nutrition_data and entry.no_nutrition_data == "on":
        return None

    if entry.identifier is None:
        return None

    if int(entry.identifier) == 0:
        return None

    if entry.created_t is None:
        return None

    if entry.product_name is None:
        return None

    # FIXME:
    #  - Analyze code (code-8, code-13, drop others)
    #  - Filter codes starting with 200
    #  - Get serving size
    #  - support other languages

    assert entry.nutriments is not None or entry.nutriments_estimated is not None

    nutriments = entry.nutriments or entry.nutriments_estimated
    created = datetime.date.fromtimestamp(entry.created_t)

    factor = 1.0

    # Calculate per-serving factor
    if entry.no_nutrition_data and entry.nutrition_data_per == "serving" and entry.serving_quantity:
        if entry.serving_quantity_unit in ["g", None]:
            factor *= 100.0 / entry.serving_quantity
        elif entry.serving_quantity_unit in ["%"]:
            factor *= entry.serving_quantity / 100.0
        elif entry.serving_quantity_unit in ["ml", "mmol/l"]:
            return None
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

    # Vitamin B9
    vitamin_b9: Optional[float] = convert_nutrient(
        value=nutriments.vitamin_b9,
        unit=nutriments.vitamin_b9_unit,
        factor=factor,
        name="vitamin_b9",
    )

    folates: Optional[float] = convert_nutrient(
        value=nutriments.folates,
        unit=nutriments.folates_unit,
        factor=factor,
        name="folates",
    )

    if vitamin_b9 is not None:
        if folates is not None:
            vitamin_b9 += folates
    elif folates is not None:
        vitamin_b9 = folates

    return OpenFoodFactsOutputEntry(
        identifier=int(entry.identifier),
        code=int(entry.code),
        created=created,
        last_updated=(
            datetime.date.fromtimestamp(entry.last_updated_t) if entry.last_updated_t else created
        ),
        name=entry.product_name,
        brands=[brand.strip() for brand in entry.brands.split(",")] if entry.brands else None,
        alcohol=alcohol,
        vitamin_b9=vitamin_b9,
        **{
            name: convert_nutrient(
                value=getattr(nutriments, name),
                unit=getattr(nutriments, f"{name}_unit"),
                factor=factor,
                name=name,
            )
            for name in REGULAR_NUTRIMENT_NAMES
        },
    ).model_dump_json(indent=4)


def import_off(file: Path):
    total = 0
    valid = 0
    with multiprocessing.Pool(multiprocessing.cpu_count()) as p, Path("output.json").open("w") as o:
        for n in tqdm.tqdm(
            p.imap(
                func=convert,
                iterable=gzip.open(file),
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
