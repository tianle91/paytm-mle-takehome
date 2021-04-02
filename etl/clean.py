from typing import Dict

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

MISSING_VALUES = {
    'TEMP': 9999.9,
    'DEWP': 9999.9,
    'SLP': 9999.9,
    'STP': 9999.9,
    'VISIB': 999.9,
    'WDSP': 999.9,
    'MXSPD': 999.9,
    'GUST': 999.9,
    'MAX': 9999.9,
    'MIN': 9999.9,
    'PRCP': 99.99,
    'SNDP': 999.9
}

DATE_FORMATS = {
    'YEARMODA': 'yyyyMMdd'
}


def nullify_missing_values(df: DataFrame, missing_values: Dict[str, object] = MISSING_VALUES) -> DataFrame:
    for c, v in missing_values.items():
        # assume >= because it doesn't make sense for missing to be between allowable values
        # also, equality was giving maximums that are equal to missing value, probably related to precision.
        df = df.withColumn(c, F.when(F.col(c) >= v, F.lit(None)).otherwise(F.col(c)))
    return df


def parse_date(df: DataFrame, date_formats: Dict[str, str] = DATE_FORMATS) -> DataFrame:
    for c, fmt in date_formats.items():
        df = df.withColumn(c, F.to_timestamp(F.col(c), fmt))
    return df
