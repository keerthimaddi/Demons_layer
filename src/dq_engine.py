from pyspark.sql import SparkSession


def check_table_exists(spark, catalog, schema, table):
    """
    DQ1: Check whether the table exists and is accessible.
    """

    full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return "PASS"

    except Exception:
        return "FAIL"


def check_row_count(spark, catalog, schema, table):
    """
    DQ2: Check whether the table contains records.
    """

    full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

    try:
        row_count = spark.table(full_table_name).count()

        if row_count > 0:
            return "PASS"
        else:
            return "FAIL"

    except Exception:
        return "FAIL"


def check_duplicate_records(spark, catalog, schema, table):
    """
    DQ3: Check whether duplicate complete rows exist.
    """

    full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

    try:
        df = spark.table(full_table_name)

        total_count = df.count()
        distinct_count = df.distinct().count()

        if total_count == distinct_count:
            return "PASS"
        else:
            return "FAIL"

    except Exception:
        return "FAIL"


def check_null_values(spark, catalog, schema, table):
    """
    DQ4: Check whether the table contains null values.
    """

    full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

    try:
        df = spark.table(full_table_name)

        for column in df.columns:

            null_count = (
                df.filter(df[column].isNull())
                .limit(1)
                .count()
            )

            if null_count > 0:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


def check_schema_availability(spark, catalog, schema, table):
    """
    DQ5: Check whether the table schema can be read.
    """

    full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

    try:
        df = spark.table(full_table_name)

        if len(df.schema.fields) > 0:
            return "PASS"

        return "FAIL"

    except Exception:
        return "FAIL"


def run_dq_checks(spark, catalog, schema, table):
    """
    Execute all DQ checks for one table.
    """

    dq1 = check_table_exists(
        spark,
        catalog,
        schema,
        table
    )

    dq2 = check_row_count(
        spark,
        catalog,
        schema,
        table
    )

    dq3 = check_duplicate_records(
        spark,
        catalog,
        schema,
        table
    )

    dq4 = check_null_values(
        spark,
        catalog,
        schema,
        table
    )

    dq5 = check_schema_availability(
        spark,
        catalog,
        schema,
        table
    )

    checks = [dq1, dq2, dq3, dq4, dq5]

    passed_checks = checks.count("PASS")

    total_checks = len(checks)

    total_score = round(
        (passed_checks / total_checks) * 100,
        2
    )

    return {
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "DQ1": dq1,
        "DQ2": dq2,
        "DQ3": dq3,
        "DQ4": dq4,
        "DQ5": dq5,
        "Total Score": total_score
    }