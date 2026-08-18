from pyspark.sql.functions import (
    col,
    isnan,
    current_timestamp,
    length
)


# ============================================================
# HELPER - LOAD TABLE
# ============================================================

def get_table(
    spark,
    catalog,
    schema,
    table
):
    """
    Return the Spark DataFrame for a Unity Catalog table.
    """

    full_table_name = (
        f"`{catalog}`.`{schema}`.`{table}`"
    )

    return spark.table(full_table_name)


# ============================================================
# HELPER - SAFE PERCENTAGE
# ============================================================

def calculate_percentage(
    invalid_count,
    total_count
):
    """
    Calculate percentage.

    Returns 0 when denominator is zero.
    """

    if total_count == 0:
        return 0.0

    return (
        float(invalid_count)
        / float(total_count)
    ) * 100.0


# ============================================================
# HELPER - STATUS FROM YAML THRESHOLD
# ============================================================

def get_status(
    failure_percentage,
    threshold
):
    """
    Convert failure percentage into:

        PASS
        WARNING
        FAIL

    using YAML thresholds.

    Example:

        pass: 0
        warning: 5
        fail: 10

    Result:

        <= 0%       PASS
        >0 - 5%     WARNING
        >5%         FAIL
    """

    try:

        if not isinstance(
            threshold,
            dict
        ):
            threshold = {}

        pass_threshold = float(
            threshold.get(
                "pass",
                0
            )
        )

        warning_threshold = float(
            threshold.get(
                "warning",
                5
            )
        )

        fail_threshold = float(
            threshold.get(
                "fail",
                10
            )
        )

        if failure_percentage <= pass_threshold:

            return "PASS"

        elif failure_percentage <= warning_threshold:

            return "WARNING"

        elif failure_percentage <= fail_threshold:

            return "FAIL"

        else:

            return "FAIL"

    except Exception:

        return "FAIL"


# ============================================================
# HELPER - GET RULE THRESHOLD
# ============================================================

def get_rule_threshold(
    rule
):
    """
    Return threshold configuration for a DQ rule.

    Supports both:

        threshold:
            pass: ...
            warning: ...
            fail: ...

    and:

        rules:
            pass: ...
            warning: ...
            fail: ...
    """

    if not isinstance(
        rule,
        dict
    ):
        return {}

    threshold = rule.get(
        "threshold"
    )

    if isinstance(
        threshold,
        dict
    ):
        return threshold

    rules_config = rule.get(
        "rules"
    )

    if isinstance(
        rules_config,
        dict
    ):
        return rules_config

    return {}


# ============================================================
# DQ01 - COMPLETENESS
# ============================================================

def check_completeness(
    df,
    threshold
):
    """
    DQ01 - Completeness

    Measures the percentage of NULL cells
    across the dataset.
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        total_columns = len(
            df.columns
        )

        if total_columns == 0:
            return "FAIL"

        total_cells = (
            total_rows
            * total_columns
        )

        null_cells = 0

        for column_name in df.columns:

            null_cells += (
                df.filter(
                    col(column_name).isNull()
                ).count()
            )

        null_percentage = calculate_percentage(
            null_cells,
            total_cells
        )

        return get_status(
            null_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ01 completeness check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ02 - ACCURACY
# ============================================================

def check_accuracy(
    df,
    threshold
):
    """
    DQ02 - Accuracy

    Current generic implementation checks
    numeric columns for NaN values.

    If no numeric columns exist, the check
    is considered PASS because there is no
    applicable numeric accuracy test.
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        numeric_columns = [
            field.name
            for field in df.schema.fields
            if field.dataType.simpleString()
            in [
                "double",
                "float",
                "decimal",
                "int",
                "bigint",
                "long",
                "short"
            ]
        ]

        if not numeric_columns:
            return "PASS"

        invalid_count = 0

        for column_name in numeric_columns:

            invalid_count += (
                df.filter(
                    isnan(
                        col(column_name)
                    )
                ).count()
            )

        total_possible = (
            total_rows
            * len(numeric_columns)
        )

        failure_percentage = calculate_percentage(
            invalid_count,
            total_possible
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ02 accuracy check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ03 - VALIDITY
# ============================================================

def check_validity(
    df,
    threshold
):
    """
    DQ03 - Validity

    Generic validation that all columns
    have valid Spark data types.
    """

    try:

        total_columns = len(
            df.columns
        )

        if total_columns == 0:
            return "FAIL"

        invalid_columns = sum(
            1
            for field in df.schema.fields
            if field.dataType is None
        )

        failure_percentage = calculate_percentage(
            invalid_columns,
            total_columns
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ03 validity check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ04 - UNIQUENESS
# ============================================================

def check_uniqueness(
    df,
    threshold,
    table_name=None
):
    """
    DQ04 - Uniqueness

    Uses configured unique keys when available.

    If no configured key exists, automatically
    detects columns ending with:

        _id
        _key
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        unique_keys = threshold.get(
            "unique_keys",
            {}
        )

        if not isinstance(
            unique_keys,
            dict
        ):
            unique_keys = {}

        configured_columns = unique_keys.get(
            table_name
        )

        # ----------------------------------------------------
        # Explicitly configured empty key list
        # ----------------------------------------------------

        if configured_columns == []:

            print(
                "DQ04: No unique key configured for "
                f"{table_name}"
            )

            return "PASS"

        # ----------------------------------------------------
        # Configured keys
        # ----------------------------------------------------

        if configured_columns:

            candidate_columns = [
                column
                for column in configured_columns
                if column in df.columns
            ]

            print(
                "DQ04 configured uniqueness columns: "
                f"{candidate_columns}"
            )

        else:

            # ------------------------------------------------
            # Automatic key detection
            # ------------------------------------------------

            candidate_columns = [
                column
                for column in df.columns
                if (
                    column.lower().endswith("_id")
                    or column.lower().endswith("_key")
                )
            ]

            print(
                "DQ04 automatically detected columns: "
                f"{candidate_columns}"
            )

        if not candidate_columns:
            return "PASS"

        total_duplicate_records = 0
        total_non_null_records = 0

        for column_name in candidate_columns:

            non_null_df = df.filter(
                col(column_name).isNotNull()
            )

            non_null_count = (
                non_null_df.count()
            )

            if non_null_count == 0:
                continue

            duplicate_groups = (
                non_null_df
                .groupBy(column_name)
                .count()
                .filter(
                    col("count") > 1
                )
            )

            duplicate_count = (
                duplicate_groups
                .selectExpr(
                    "coalesce(sum(count), 0) "
                    "as duplicate_count"
                )
                .collect()[0]["duplicate_count"]
            )

            if duplicate_count is None:
                duplicate_count = 0

            total_duplicate_records += (
                duplicate_count
            )

            total_non_null_records += (
                non_null_count
            )

        if total_non_null_records == 0:
            return "PASS"

        duplicate_percentage = calculate_percentage(
            total_duplicate_records,
            total_non_null_records
        )

        print(
            f"DQ04 duplicate percentage: "
            f"{duplicate_percentage:.2f}%"
        )

        return get_status(
            duplicate_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ04 uniqueness check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ05 - CONSISTENCY
# ============================================================

def check_consistency(
    df,
    threshold
):
    """
    DQ05 - Consistency

    Checks common consistency relationships:

        start <= end
        quantity >= 0
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        invalid_rows = 0
        checks_performed = 0

        start_columns = [
            c
            for c in df.columns
            if "start" in c.lower()
        ]

        end_columns = [
            c
            for c in df.columns
            if "end" in c.lower()
        ]

        if start_columns and end_columns:

            start_col = start_columns[0]
            end_col = end_columns[0]

            checks_performed += 1

            invalid_rows += (
                df.filter(
                    col(start_col)
                    > col(end_col)
                ).count()
            )

        quantity_columns = [
            c
            for c in df.columns
            if (
                "quantity" in c.lower()
                or "qty" in c.lower()
            )
        ]

        for column_name in quantity_columns:

            checks_performed += 1

            invalid_rows += (
                df.filter(
                    col(column_name) < 0
                ).count()
            )

        if checks_performed == 0:
            return "PASS"

        total_possible = (
            total_rows
            * checks_performed
        )

        failure_percentage = calculate_percentage(
            invalid_rows,
            total_possible
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ05 consistency check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ06 - INTEGRITY
# ============================================================

def check_integrity(
    df,
    threshold
):
    """
    DQ06 - Integrity

    Current generic implementation checks
    ID/key columns for NULL values.

    Cross-table referential integrity will be
    enhanced when relationship configuration
    is added to the YAML.
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        key_columns = [
            c
            for c in df.columns
            if (
                "id" in c.lower()
                or "key" in c.lower()
            )
        ]

        if not key_columns:
            return "PASS"

        total_nulls = 0

        for column_name in key_columns:

            total_nulls += (
                df.filter(
                    col(column_name).isNull()
                ).count()
            )

        total_possible = (
            total_rows
            * len(key_columns)
        )

        failure_percentage = calculate_percentage(
            total_nulls,
            total_possible
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ06 integrity check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ07 - TIMELINESS
# ============================================================

def check_timeliness(
    df,
    threshold
):
    """
    DQ07 - Timeliness

    Checks date/timestamp columns for
    future values.
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        date_columns = [
            field.name
            for field in df.schema.fields
            if (
                "date" in field.name.lower()
                or "time" in field.name.lower()
                or "timestamp" in field.name.lower()
            )
        ]

        if not date_columns:
            return "PASS"

        print(
            f"DQ07 date/timestamp columns: "
            f"{date_columns}"
        )

        future_count = 0

        for column_name in date_columns:

            future_count += (
                df.filter(
                    col(column_name)
                    > current_timestamp()
                ).count()
            )

        total_possible = (
            total_rows
            * len(date_columns)
        )

        failure_percentage = calculate_percentage(
            future_count,
            total_possible
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ07 timeliness check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ08 - CONFORMITY
# ============================================================

def check_conformity(
    df,
    threshold
):
    """
    DQ08 - Conformity

    Current generic implementation checks
    column names for spaces.
    """

    try:

        total_columns = len(
            df.columns
        )

        if total_columns == 0:
            return "FAIL"

        invalid_columns = sum(
            1
            for column_name in df.columns
            if " " in column_name
        )

        failure_percentage = calculate_percentage(
            invalid_columns,
            total_columns
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ08 conformity check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ09 - RANGE
# ============================================================

def check_range(
    df,
    threshold
):
    """
    DQ09 - Range

    Checks common business numeric fields
    for negative values.
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

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

        business_columns = [
            c
            for c in numeric_columns
            if any(
                keyword in c.lower()
                for keyword in [
                    "amount",
                    "revenue",
                    "price",
                    "cost",
                    "spend",
                    "quantity",
                    "count"
                ]
            )
        ]

        if not business_columns:
            return "PASS"

        invalid_values = 0

        for column_name in business_columns:

            invalid_values += (
                df.filter(
                    col(column_name) < 0
                ).count()
            )

        total_possible = (
            total_rows
            * len(business_columns)
        )

        failure_percentage = calculate_percentage(
            invalid_values,
            total_possible
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ09 range check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ10 - DUPLICATE
# ============================================================

def check_duplicate(
    df,
    threshold
):
    """
    DQ10 - Duplicate

    Checks complete duplicate rows.
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        distinct_count = (
            df.distinct().count()
        )

        duplicate_count = (
            total_rows
            - distinct_count
        )

        failure_percentage = calculate_percentage(
            duplicate_count,
            total_rows
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ10 duplicate check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ11 - NULL
# ============================================================

def check_null(
    df,
    threshold
):
    """
    DQ11 - Null

    Measures the percentage of NULL cells.
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        total_columns = len(
            df.columns
        )

        if total_columns == 0:
            return "FAIL"

        total_cells = (
            total_rows
            * total_columns
        )

        null_cells = 0

        for column_name in df.columns:

            null_cells += (
                df.filter(
                    col(column_name).isNull()
                ).count()
            )

        failure_percentage = calculate_percentage(
            null_cells,
            total_cells
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ11 null check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ12 - LENGTH
# ============================================================

def check_length(
    df,
    threshold
):
    """
    DQ12 - Length

    Checks string columns against max_length.
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        string_columns = [
            field.name
            for field in df.schema.fields
            if field.dataType.simpleString()
            == "string"
        ]

        if not string_columns:
            return "PASS"

        max_length = threshold.get(
            "max_length",
            1000
        )

        try:

            max_length = int(
                max_length
            )

        except (
            TypeError,
            ValueError
        ):

            max_length = 1000

        invalid_values = 0

        for column_name in string_columns:

            invalid_values += (
                df.filter(
                    col(column_name).isNotNull()
                    & (
                        length(
                            col(column_name)
                        )
                        > max_length
                    )
                ).count()
            )

        total_possible = (
            total_rows
            * len(string_columns)
        )

        failure_percentage = calculate_percentage(
            invalid_values,
            total_possible
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ12 length check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ13 - DATA TYPE
# ============================================================

def check_data_type(
    df,
    threshold
):
    """
    DQ13 - Data Type

    Generic validation that every column
    has an identifiable Spark data type.
    """

    try:

        total_columns = len(
            df.columns
        )

        if total_columns == 0:
            return "FAIL"

        invalid_columns = sum(
            1
            for field in df.schema.fields
            if field.dataType is None
        )

        failure_percentage = calculate_percentage(
            invalid_columns,
            total_columns
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ13 data type check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ14 - PATTERN
# ============================================================

def check_pattern(
    df,
    threshold
):
    """
    DQ14 - Pattern

    Current generic implementation validates
    email columns using '@'.
    """

    try:

        email_columns = [
            c
            for c in df.columns
            if "email" in c.lower()
        ]

        if not email_columns:
            return "PASS"

        total_checked = 0
        invalid_values = 0

        for column_name in email_columns:

            non_null_count = (
                df.filter(
                    col(column_name).isNotNull()
                ).count()
            )

            invalid_count = (
                df.filter(
                    col(column_name).isNotNull()
                    & ~col(column_name).contains("@")
                ).count()
            )

            total_checked += non_null_count
            invalid_values += invalid_count

        if total_checked == 0:
            return "PASS"

        failure_percentage = calculate_percentage(
            invalid_values,
            total_checked
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ14 pattern check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ15 - BUSINESS RULE
# ============================================================

def check_business_rule(
    df,
    threshold
):
    """
    DQ15 - Business Rule

    Current generic implementation checks
    common financial fields for negative values.
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        business_columns = [
            c
            for c in df.columns
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

        if not business_columns:
            return "PASS"

        invalid_values = 0

        for column_name in business_columns:

            invalid_values += (
                df.filter(
                    col(column_name) < 0
                ).count()
            )

        total_possible = (
            total_rows
            * len(business_columns)
        )

        failure_percentage = calculate_percentage(
            invalid_values,
            total_possible
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ15 business rule check failed: {error}"
        )

        return "FAIL"


# ============================================================
# DQ16 - VOLUME
# ============================================================

def check_volume(
    df,
    threshold,
    baseline_row_count=None
):
    """
    DQ16 - Volume

    If a historical baseline exists:

        abs(current - baseline)
        ------------------------ * 100
              baseline

    If no baseline exists, a non-empty table
    is currently treated as PASS.
    """

    try:

        current_row_count = df.count()

        if current_row_count == 0:
            return "FAIL"

        if baseline_row_count is None:
            return "PASS"

        baseline_row_count = float(
            baseline_row_count
        )

        if baseline_row_count <= 0:
            return "FAIL"

        volume_change_percentage = (
            abs(
                current_row_count
                - baseline_row_count
            )
            / baseline_row_count
        ) * 100.0

        return get_status(
            volume_change_percentage,
            threshold
        )

    except Exception as error:

        print(
            f"DQ16 volume check failed: {error}"
        )

        return "FAIL"


# ============================================================
# MAIN DQ ENGINE
# ============================================================

def run_dq_checks(
    spark,
    catalog,
    schema,
    table,
    rules,
    overall_thresholds,
    baseline_row_count=None
):
    """
    Execute all enabled DQ rules.

    IMPORTANT:
        Rule IDs MUST match YAML exactly.

        DQ01
        DQ02
        ...
        DQ16

    Weighted scoring:

        PASS    = 100% of weight
        WARNING = 50% of weight
        FAIL    = 0% of weight
    """

    full_table_name = (
        f"`{catalog}`.`{schema}`.`{table}`"
    )

    # ========================================================
    # LOAD TABLE
    # ========================================================

    try:

        df = spark.table(
            full_table_name
        )

    except Exception as error:

        print(
            f"Unable to load table "
            f"{catalog}.{schema}.{table}: "
            f"{error}"
        )

        result = {
            "catalog": catalog,
            "schema": schema,
            "table": table
        }

        for rule in rules:

            if rule.get(
                "enabled",
                False
            ):

                result[
                    rule["id"]
                ] = "FAIL"

        result["Total Score"] = 0.0
        result["Overall Status"] = "FAIL"

        return result

    # ========================================================
    # RULE MAP
    # ========================================================

    rule_map = {
        rule["id"]: rule
        for rule in rules
    }

    # ========================================================
    # THRESHOLD HELPER
    # ========================================================

    def threshold_for(
        dq_id
    ):

        rule = rule_map.get(
            dq_id,
            {}
        )

        return get_rule_threshold(
            rule
        )

    # ========================================================
    # EXECUTE DQ RULES
    # ========================================================

    dq_results = {}

    # --------------------------------------------------------
    # DQ01
    # --------------------------------------------------------

    if "DQ01" in rule_map:

        dq_results["DQ01"] = check_completeness(
            df,
            threshold_for("DQ01")
        )

    # --------------------------------------------------------
    # DQ02
    # --------------------------------------------------------

    if "DQ02" in rule_map:

        dq_results["DQ02"] = check_accuracy(
            df,
            threshold_for("DQ02")
        )

    # --------------------------------------------------------
    # DQ03
    # --------------------------------------------------------

    if "DQ03" in rule_map:

        dq_results["DQ03"] = check_validity(
            df,
            threshold_for("DQ03")
        )

    # --------------------------------------------------------
    # DQ04
    # --------------------------------------------------------

    if "DQ04" in rule_map:

        dq_results["DQ04"] = check_uniqueness(
            df,
            threshold_for("DQ04"),
            f"{catalog}.{schema}.{table}"
        )

    # --------------------------------------------------------
    # DQ05
    # --------------------------------------------------------

    if "DQ05" in rule_map:

        dq_results["DQ05"] = check_consistency(
            df,
            threshold_for("DQ05")
        )

    # --------------------------------------------------------
    # DQ06
    # --------------------------------------------------------

    if "DQ06" in rule_map:

        dq_results["DQ06"] = check_integrity(
            df,
            threshold_for("DQ06")
        )

    # --------------------------------------------------------
    # DQ07
    # --------------------------------------------------------

    if "DQ07" in rule_map:

        dq_results["DQ07"] = check_timeliness(
            df,
            threshold_for("DQ07")
        )

    # --------------------------------------------------------
    # DQ08
    # --------------------------------------------------------

    if "DQ08" in rule_map:

        dq_results["DQ08"] = check_conformity(
            df,
            threshold_for("DQ08")
        )

    # --------------------------------------------------------
    # DQ09
    # --------------------------------------------------------

    if "DQ09" in rule_map:

        dq_results["DQ09"] = check_range(
            df,
            threshold_for("DQ09")
        )

    # --------------------------------------------------------
    # DQ10
    # --------------------------------------------------------

    if "DQ10" in rule_map:

        dq_results["DQ10"] = check_duplicate(
            df,
            threshold_for("DQ10")
        )

    # --------------------------------------------------------
    # DQ11
    # --------------------------------------------------------

    if "DQ11" in rule_map:

        dq_results["DQ11"] = check_null(
            df,
            threshold_for("DQ11")
        )

    # --------------------------------------------------------
    # DQ12
    # --------------------------------------------------------

    if "DQ12" in rule_map:

        dq_results["DQ12"] = check_length(
            df,
            threshold_for("DQ12")
        )

    # --------------------------------------------------------
    # DQ13
    # --------------------------------------------------------

    if "DQ13" in rule_map:

        dq_results["DQ13"] = check_data_type(
            df,
            threshold_for("DQ13")
        )

    # --------------------------------------------------------
    # DQ14
    # --------------------------------------------------------

    if "DQ14" in rule_map:

        dq_results["DQ14"] = check_pattern(
            df,
            threshold_for("DQ14")
        )

    # --------------------------------------------------------
    # DQ15
    # --------------------------------------------------------

    if "DQ15" in rule_map:

        dq_results["DQ15"] = check_business_rule(
            df,
            threshold_for("DQ15")
        )

    # --------------------------------------------------------
    # DQ16
    # --------------------------------------------------------

    if "DQ16" in rule_map:

        dq_results["DQ16"] = check_volume(
            df,
            threshold_for("DQ16"),
            baseline_row_count
        )

    # ========================================================
    # ENABLED RULES
    # ========================================================

    enabled_rules = [
        rule
        for rule in rules
        if rule.get(
            "enabled",
            False
        )
    ]

    # ========================================================
    # TOTAL ENABLED WEIGHT
    # ========================================================

    total_enabled_weight = sum(
        float(
            rule.get(
                "default_weight",
                0
            )
        )
        for rule in enabled_rules
    )

    # ========================================================
    # EARNED WEIGHT
    # ========================================================

    earned_weight = 0.0

    for rule in enabled_rules:

        rule_id = rule["id"]

        weight = float(
            rule.get(
                "default_weight",
                0
            )
        )

        status = dq_results.get(
            rule_id,
            "FAIL"
        )

        if status == "PASS":

            earned_weight += weight

        elif status == "WARNING":

            earned_weight += (
                weight * 0.5
            )

        elif status == "FAIL":

            earned_weight += 0.0

    # ========================================================
    # TOTAL SCORE
    # ========================================================

    if total_enabled_weight > 0:

        total_score = round(
            (
                earned_weight
                / total_enabled_weight
            ) * 100,
            2
        )

    else:

        total_score = 0.0

    # ========================================================
    # OVERALL STATUS
    # ========================================================

    if not isinstance(
        overall_thresholds,
        dict
    ):

        overall_thresholds = {
            "pass": 90,
            "warning": 75,
            "fail": 0
        }

    pass_threshold = float(
        overall_thresholds.get(
            "pass",
            90
        )
    )

    warning_threshold = float(
        overall_thresholds.get(
            "warning",
            75
        )
    )

    if total_score >= pass_threshold:

        overall_status = "PASS"

    elif total_score >= warning_threshold:

        overall_status = "WARNING"

    else:

        overall_status = "FAIL"

    # ========================================================
    # FINAL RESULT
    # ========================================================

    result = {
        "catalog": catalog,
        "schema": schema,
        "table": table
    }

    for rule in enabled_rules:

        rule_id = rule["id"]

        result[rule_id] = dq_results.get(
            rule_id,
            "FAIL"
        )

    result["Total Score"] = total_score

    result["Overall Status"] = overall_status

    return result