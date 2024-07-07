from pyspark.sql.functions import *

def trim_all_string_columns(df : DataFrame) -> DataFrame:
    return (
        df.select(*[trim(col(c[0]).alias(c[0]) if c[1] == 'string' else col[0]) for c in df.dtypes])
        )

