import os
from typing import Tuple

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (FloatType, IntegerType, StringType, StructField,
                               StructType)

read_csv_options = {'header': True}


schema = StructType([
    # index columns
    StructField('STN---', IntegerType(), False),
    StructField('WBAN', IntegerType(), False),
    # not IntegerType as described in data dictionary because to_timestamp expects StringType
    # also, what if we get observations from the year 100?
    StructField('YEARMODA', StringType(), False),
    # following are data columns, nullable
    *[
        StructField(s, FloatType(), True)
        for s in ['TEMP', 'DEWP', 'SLP', 'STP', 'VISIB', 'WDSP', 'MXSPD', 'GUST', 'MAX', 'MIN', 'PRCP', 'SNDP']
    ],
    StructField('FRSHTT', StringType(), True),
])


def load(spark: SparkSession, input_prefix: str) -> Tuple[DataFrame, DataFrame, DataFrame]:
    countrylist = (
        spark
        .read
        .options(**read_csv_options)
        .csv(os.path.join(input_prefix, 'countrylist.csv'))
    )
    stationlist = (
        spark
        .read
        .options(**read_csv_options)
        .csv(os.path.join(input_prefix, 'stationlist.csv'))
    )
    data = (
        spark
        .read
        .options(**read_csv_options)
        .schema(schema)
        .csv(os.path.join(input_prefix, 'data', '2019'))
    )
    return countrylist, stationlist, data
