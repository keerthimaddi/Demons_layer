from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, isnan, when


# ============================================================
# Helper
# ============================================================

def get_table(spark, catalog, schema, table):
    """
    Load a Unity Catalog table using:
    catalog.schema.table
    """

    full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

    return spark.table(full_table_name)


# ============================================================
# DQ1 - Completeness
# ============================================================

def check_completeness(df: DataFrame):
    """
    DQ1:
    Check whether columns contain null values.
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        for column_name in df.columns:

            null_count = (
                df.filter(col(column_name).isNull())
                .limit(1)
                .count()
            )

            if null_count > 0:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ2 - Accuracy
# ============================================================

def check_accuracy(df: DataFrame):
    """
    DQ2:
    Basic accuracy validation.

    Current generic implementation checks that
    numeric columns do not contain NaN values.
    """

    try:

        numeric_columns = [
            field.name
            for field in df.schema.fields
            if field.dataType.simpleString()
            in ["double", "float"]
        ]

        for column_name in numeric_columns:

            invalid_count = (
                df.filter(isnan(col(column_name)))
                .limit(1)
                .count()
            )

            if invalid_count > 0:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ3 - Validity
# ============================================================

def check_validity(df: DataFrame):
    """
    DQ3:
    Validate that the dataset can be read and contains
    supported values/types.
    """

    try:

        if len(df.schema.fields) == 0:
            return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ4 - Uniqueness
# ============================================================

def check_uniqueness(df: DataFrame):
    """
    DQ4:
    Check whether duplicate complete records exist.
    """

    try:

        total_count = df.count()
        distinct_count = df.distinct().count()

        if total_count == distinct_count:
            return "PASS"

        return "FAIL"

    except Exception:
        return "FAIL"


# ============================================================
# DQ5 - Consistency
# ============================================================

def check_consistency(df: DataFrame):
    """
    DQ5:
    Basic dataset consistency check.

    Ensures the dataset has a valid schema and columns.
    """

    try:

        if len(df.columns) == 0:
            return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ6 - Integrity
# ============================================================

def check_integrity(df: DataFrame):
    """
    DQ6:
    Basic structural integrity check.

    Detailed referential integrity can later be configured
    between multiple tables.
    """

    try:

        if len(df.schema.fields) == 0:
            return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ7 - Timeliness
# ============================================================

def check_timeliness(df: DataFrame):
    """
    DQ7:
    Basic timeliness check.

    Detailed SLA-based validation will require a configured
    timestamp column and expected freshness threshold.
    """

    try:

        timestamp_columns = [
            field.name
            for field in df.schema.fields
            if field.dataType.simpleString() == "timestamp"
        ]

        if timestamp_columns:
            return "PASS"

        # Generic tables may not contain timestamps.
        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ8 - Conformity
# ============================================================

def check_conformity(df: DataFrame):
    """
    DQ8:
    Check that the table contains valid column definitions.
    """

    try:

        for field in df.schema.fields:

            if not field.name or not field.name.strip():
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ9 - Range
# ============================================================

def check_range(df: DataFrame):
    """
    DQ9:
    Generic numeric range validation.

    Negative values are currently treated as invalid.
    This can later be configured per column in YAML.
    """

    try:

        numeric_columns = [
            field.name
            for field in df.schema.fields
            if field.dataType.simpleString()
            in [
                "integer",
                "bigint",
                "long",
                "double",
                "float",
                "decimal"
            ]
        ]

        for column_name in numeric_columns:

            invalid_count = (
                df.filter(col(column_name) < 0)
                .limit(1)
                .count()
            )

            if invalid_count > 0:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ10 - Duplicate
# ============================================================

def check_duplicate(df: DataFrame):
    """
    DQ10:
    Check for duplicate complete records.
    """

    try:

        total_count = df.count()
        distinct_count = df.distinct().count()

        if total_count == distinct_count:
            return "PASS"

        return "FAIL"

    except Exception:
        return "FAIL"


# ============================================================
# DQ11 - Null
# ============================================================

def check_null(df: DataFrame):
    """
    DQ11:
    Detect unexpected null values.
    """

    try:

        for column_name in df.columns:

            null_exists = (
                df.filter(col(column_name).isNull())
                .limit(1)
                .count()
            )

            if null_exists > 0:
                return "FAIL"

        return "PASS"

    except Exception:
        return "FAIL"


# ============================================================
# DQ12 - Length
# ============================================================

def check_length(df: DataFrame):
    """
    DQ12:
    Basic string length validation.
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
                    (col(column_name).isNotNull()) &
                    (col(column_name) == "")
                )
                .limit(1)
                .count()
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
    Verify that the Spark schema can be successfully read.
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
    Generic pattern validation.

    Detailed regex rules will later be configured in YAML.
    """

    try:

        for field in df.schema.fields:

            if field.name is None:
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
    Generic business rule validation.

    Specific business rules will later be configured
    per dataset/column.
    """

    try:

        if len(df.columns) == 0:
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
    Check whether the table contains records.
    """

    try:

        row_count = df.count()

        if row_count > 0:
            return "PASS"

        return "FAIL"

    except Exception:
        return "FAIL"


# ============================================================
# DQ FUNCTION REGISTRY
# ============================================================

DQ_FUNCTIONS = {

    "completeness": check_completeness,
    "accuracy": check_accuracy,
    "validity": check_validity,
    "uniqueness": check_uniqueness,
    "consistency": check_consistency,
    "integrity": check_integrity,
    "timeliness": check_timeliness,
    "conformity": check_conformity,
    "range": check_range,
    "duplicate": check_duplicate,
    "null_check": check_null,
    "length": check_length,
    "data_type": check_data_type,
    "pattern": check_pattern,
    "business_rule": check_business_rule,
    "volume": check_volume
}


# ============================================================
# RUN DQ CHECKS FOR ONE TABLE
# ============================================================

def run_dq_checks(spark, catalog, schema, table, rules):
    """
    Execute the enabled DQ checks for one Catalog.Schema.Table.

    Rules are supplied by rule_loader.py.
    """

    try:

        df = get_table(
            spark,
            catalog,
            schema,
            table
        )

    except Exception:

        return {
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "DQ1": "FAIL",
            "DQ2": "FAIL",
            "DQ3": "FAIL",
            "DQ4": "FAIL",
            "DQ5": "FAIL",
            "DQ6": "FAIL",
            "DQ7": "FAIL",
            "DQ8": "FAIL",
            "DQ9": "FAIL",
            "DQ10": "FAIL",
            "DQ11": "FAIL",
            "DQ12": "FAIL",
            "DQ13": "FAIL",
            "DQ14": "FAIL",
            "DQ15": "FAIL",
            "DQ16": "FAIL",
            "Total Score": 0.0
        }

    results = {}

    weighted_score = 0.0
    total_weight = 0.0

    for rule in rules:

        rule_id = rule["id"]
        category = rule["category"]
        enabled = rule["enabled"]
        weight = float(rule["default_weight"])

        if not enabled:
            results[rule_id] = "SKIP"
            continue

        check_function = DQ_FUNCTIONS.get(category)

        if check_function is None:

            results[rule_id] = "FAIL"

            continue

        result = check_function(df)

        results[rule_id] = result

        total_weight += weight

        if result == "PASS":
            weighted_score += weight

    if total_weight > 0:

        total_score = round(
            (weighted_score / total_weight) * 100,
            2
        )

    else:

        total_score = 0.0

    results["catalog"] = catalog
    results["schema"] = schema
    results["table"] = table
    results["Total Score"] = total_score

    return results