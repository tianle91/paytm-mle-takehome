from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


def consecutive_date_diff_by(df: DataFrame, by_cols: List[str], date_col: str) -> DataFrame:
    w = Window.partitionBy(*by_cols).orderBy(date_col)
    df = (
        df
        .select(*by_cols, date_col)
        .distinct()
        .withColumn(
            'inter_date',
            F.datediff(
                F.col(date_col),
                F.lag(F.col(date_col), 1).over(w)
            )
        )
        .orderBy(*by_cols, date_col)
    )
    return df
