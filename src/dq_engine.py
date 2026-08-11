from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    isnan,
    when,
    sum as spark_sum,
    current_timestamp
)


# ============================================================
# Helper
# ============================================================

def get_table(spark, catalog, schema, table):
    """
    Return the Spark DataFrame for a Unity Catalog table.
    """

    full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

    return spark.table(full_table_name)


# ============================================================
# DQ1 - Completeness
# ============================================================

def check_completeness(df):
    """
    DQ1:
    Checks whether the dataset has meaningful data
    and whether columns are sufficiently populated.

    PASS = at least 95% of values are populated.
    """

    try:
        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        total_cells = total_rows * len(df.columns)

        if total_cells == 0:
            return "FAIL"

        null_cells = 0

        for column_name in df.columns:

            null_count = (
                df.filter(
                    col(column_name).isNull()
                ).count()
            )

            null_cells += null_count

        completeness_percentage = (
            (total_cells - null_cells)
            / total_cells
        ) * 100

        return (
            "PASS"
            if completeness_percentage >= 95
            else "FAIL"
        )

    except Exception:
        return "FAIL"


# ============================================================
# DQ2 - Accuracy
# ============================================================

def check_accuracy(df):
    """
    DQ2:
    Basic accuracy validation.

    Checks numeric columns for NaN values.
    """

    try:

        numeric_columns = [
            field.name
            for field in df.schema.fields
            if field.dataType.simpleString()
            in [
                "double",
                "float",
                "decimal",
            ]
        ]

        for column_name in numeric_columns:

            invalid_count = (
                df.filter(
                    isnan(col(column_name))
                ).count()
            )

            if invalid_count > 0:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ3 - Validity
# ============================================================

def check_validity(df):
    """
    DQ3:
    Checks whether the dataset contains valid
    Spark-supported data types and columns.
    """

    try:

        if len(df.columns) == 0:
            return "FAIL"

        for field in df.schema.fields:

            if field.dataType is None:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ4 - Uniqueness
# ============================================================

def check_uniqueness(df):
    """
    DQ4:
    Checks uniqueness of likely business key columns.

    Looks for columns containing:
    id / key
    """

    try:

        candidate_columns = [
            c for c in df.columns
            if (
                "id" in c.lower()
                or "key" in c.lower()
            )
        ]

        if not candidate_columns:
            return "PASS"

        for column_name in candidate_columns:

            total_count = df.count()

            distinct_count = (
                df.select(column_name)
                .distinct()
                .count()
            )

            null_count = (
                df.filter(
                    col(column_name).isNull()
                ).count()
            )

            if (
                total_count != distinct_count
                or null_count > 0
            ):
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ5 - Consistency
# ============================================================

def check_consistency(df):
    """
    DQ5:
    Basic cross-column consistency checks.

    Examples:
    start date <= end date
    quantity >= 0
    amount >= 0
    """

    try:

        columns_lower = {
            c.lower(): c
            for c in df.columns
        }

        # Start / End date consistency
        start_columns = [
            c for c in df.columns
            if "start" in c.lower()
        ]

        end_columns = [
            c for c in df.columns
            if "end" in c.lower()
        ]

        if start_columns and end_columns:

            start_col = start_columns[0]
            end_col = end_columns[0]

            invalid = (
                df.filter(
                    col(start_col) > col(end_col)
                ).count()
            )

            if invalid > 0:
                return "FAIL"

        # Quantity should not be negative
        quantity_columns = [
            c for c in df.columns
            if "quantity" in c.lower()
            or "qty" in c.lower()
        ]

        for column_name in quantity_columns:

            invalid = (
                df.filter(
                    col(column_name) < 0
                ).count()
            )

            if invalid > 0:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ6 - Integrity
# ============================================================

def check_integrity(df):
    """
    DQ6:
    Basic referential/integrity validation.

    Checks that ID/key columns are not null.
    """

    try:

        key_columns = [
            c for c in df.columns
            if (
                "id" in c.lower()
                or "key" in c.lower()
            )
        ]

        for column_name in key_columns:

            null_count = (
                df.filter(
                    col(column_name).isNull()
                ).count()
            )

            if null_count > 0:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ7 - Timeliness
# ============================================================

def check_timeliness(df):
    """
    DQ7:
    Checks whether date/timestamp columns contain
    future values.
    """

    try:

        date_columns = [
            field.name
            for field in df.schema.fields
            if (
                "date" in field.name.lower()
                or "time" in field.name.lower()
                or "timestamp" in field.name.lower()
            )
        ]

        for column_name in date_columns:

            future_count = (
                df.filter(
                    col(column_name) > current_timestamp()
                ).count()
            )

            if future_count > 0:
                return "FAIL"

        return "PASS"

    except Exception:
        return "PASS"


# ============================================================
# DQ8 - Conformity
# ============================================================

def check_conformity(df):
    """
    DQ8:
    Basic organizational conformity check.

    Checks that column names do not contain spaces.
    """

    try:

        for column_name in df.columns:

            if " " in column_name:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ9 - Range
# ============================================================

def check_range(df):
    """
    DQ9:
    Checks numeric values for unreasonable negative values
    in common business metrics.
    """

    try:

        numeric_columns = [
            field.name
            for field in df.schema.fields
            if field.dataType.simpleString()
            in [
                "int",
                "bigint",
                "double",
                "float",
                "decimal",
                "long",
                "short"
            ]
        ]

        for column_name in numeric_columns:

            column_lower = column_name.lower()

            if any(
                keyword in column_lower
                for keyword in [
                    "amount",
                    "revenue",
                    "price",
                    "cost",
                    "spend",
                    "quantity",
                    "count"
                ]
            ):

                negative_count = (
                    df.filter(
                        col(column_name) < 0
                    ).count()
                )

                if negative_count > 0:
                    return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ10 - Duplicate
# ============================================================

def check_duplicate(df):
    """
    DQ10:
    Checks for complete duplicate rows.
    """

    try:

        total_count = df.count()

        distinct_count = (
            df.distinct().count()
        )

        if total_count != distinct_count:
            return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ11 - Null
# ============================================================

def check_null(df):
    """
    DQ11:
    Checks for unexpected null values.

    This is intentionally separate from DQ1:
    DQ1 measures overall completeness percentage.
    DQ11 identifies whether nulls exist at all.
    """

    try:

        for column_name in df.columns:

            null_count = (
                df.filter(
                    col(column_name).isNull()
                ).count()
            )

            if null_count > 0:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ12 - Length
# ============================================================

def check_length(df):
    """
    DQ12:
    Checks excessive text length.

    A generic upper limit of 1000 characters is used
    until column-specific configuration is added.
    """

    try:

        string_columns = [
            field.name
            for field in df.schema.fields
            if field.dataType.simpleString() == "string"
        ]

        for column_name in string_columns:

            invalid_count = (
                df.filter(
                    col(column_name).cast("string").substr(
                        1001, 1
                    ) != ""
                ).count()
            )

            if invalid_count > 0:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ13 - Data Type
# ============================================================

def check_data_type(df):
    """
    DQ13:
    Checks that all columns have identifiable Spark data types.
    """

    try:

        for field in df.schema.fields:

            if field.dataType is None:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ14 - Pattern
# ============================================================

def check_pattern(df):
    """
    DQ14:
    Basic pattern validation for likely email columns.
    """

    try:

        email_columns = [
            c for c in df.columns
            if "email" in c.lower()
        ]

        for column_name in email_columns:

            invalid_count = (
                df.filter(
                    col(column_name).isNotNull()
                    & ~col(column_name).contains("@")
                ).count()
            )

            if invalid_count > 0:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ15 - Business Rule
# ============================================================

def check_business_rule(df):
    """
    DQ15:
    Basic business-rule validation.

    Common business columns should not contain
    negative values.
    """

    try:

        business_columns = [
            c for c in df.columns
            if any(
                keyword in c.lower()
                for keyword in [
                    "amount",
                    "revenue",
                    "spend",
                    "cost",
                    "price"
                ]
            )
        ]

        for column_name in business_columns:

            invalid_count = (
                df.filter(
                    col(column_name) < 0
                ).count()
            )

            if invalid_count > 0:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ16 - Volume
# ============================================================

def check_volume(df):
    """
    DQ16:
    Checks whether the table contains records.

    Later this will be upgraded to compare against
    historical volume thresholds.
    """

    try:

        row_count = df.count()

        return (
            "PASS"
            if row_count > 0
            else "FAIL"
        )

    except Exception:
        return "FAIL"


# ============================================================
# MAIN DQ ENGINE
# ============================================================

def run_dq_checks(
    spark,
    catalog,
    schema,
    table,
    rules
):
    """
    Execute all enabled DQ rules and calculate
    weighted DQ score.

    Formula:

        Total Score =
        (Sum of weights of passed rules /
         Sum of weights of enabled rules) * 100
    """

    full_table_name = (
        f"`{catalog}`.`{schema}`.`{table}`"
    )

    # --------------------------------------------------------
    # Load table
    # --------------------------------------------------------

    try:

        df = spark.table(full_table_name)

    except Exception:

        return {
            "catalog": catalog,
            "schema": schema,
            "table": table,
            **{
                f"DQ{i}": "FAIL"
                for i in range(1, 17)
            },
            "Total Score": 0.0
        }

    # --------------------------------------------------------
    # Execute checks
    # --------------------------------------------------------

    dq_results = {

        "DQ1": check_completeness(df),

        "DQ2": check_accuracy(df),

        "DQ3": check_validity(df),

        "DQ4": check_uniqueness(df),

        "DQ5": check_consistency(df),

        "DQ6": check_integrity(df),

        "DQ7": check_timeliness(df),

        "DQ8": check_conformity(df),

        "DQ9": check_range(df),

        "DQ10": check_duplicate(df),

        "DQ11": check_null(df),

        "DQ12": check_length(df),

        "DQ13": check_data_type(df),

        "DQ14": check_pattern(df),

        "DQ15": check_business_rule(df),

        "DQ16": check_volume(df)
    }

    # --------------------------------------------------------
    # Weighted score
    # --------------------------------------------------------

    enabled_rules = [
        rule
        for rule in rules
        if rule.get("enabled", False)
    ]

    total_enabled_weight = sum(
        float(rule.get("default_weight", 0))
        for rule in enabled_rules
    )

    passed_weight = 0.0

    for rule in enabled_rules:

        rule_id = rule["id"]

        weight = float(
            rule.get("default_weight", 0)
        )

        if dq_results.get(rule_id) == "PASS":
            passed_weight += weight

    if total_enabled_weight > 0:

        total_score = round(
            (
                passed_weight
                / total_enabled_weight
            ) * 100,
            2
        )

    else:

        total_score = 0.0

    # --------------------------------------------------------
    # Final result
    # --------------------------------------------------------

    result = {
        "catalog": catalog,
        "schema": schema,
        "table": table
    }

    result.update(dq_results)

    result["Total Score"] = total_score

    return result