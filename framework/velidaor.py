from pyspark.sql.functions import col


class DataValidator:

    def __init__(self, spark):
        self.spark = spark

    def validate_not_null(self, df, columns):
        """
        Ensure critical columns are not null
        """
        for c in columns:
            null_count = df.filter(col(c).isNull()).count()

            if null_count > 0:
                raise Exception(f"Column {c} has {null_count} null values")

        return df

    def validate_range(self, df, column, min_val, max_val):
        """
        Validate numeric ranges
        """
        invalid = df.filter((col(column) < min_val) | (col(column) > max_val)).count()

        if invalid > 0:
            raise Exception(f"{invalid} rows out of range for {column}")

        return df

    def validate_schema(self, df, expected_cols):
        """
        Ensure schema consistency
        """
        df_cols = set(df.columns)

        missing = set(expected_cols) - df_cols

        if missing:
            raise Exception(f"Missing columns: {missing}")

        return df