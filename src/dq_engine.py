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

    using the YAML threshold.

    Example:

        pass: 0
        warning: 5
        fail: 10

    Result:

        <= 0%       PASS
        >0 - 5%     WARNING
        >5%         FAIL

    The configured 'fail' threshold is retained as the
    hard-fail boundary. Values above it are also FAIL.
    """

    try:

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

def get_rule_threshold(rule):
    """
    Return threshold configuration for a DQ rule.
    """

    threshold = rule.get(
        "threshold",
        {}
    )

    if not isinstance(
        threshold,
        dict
    ):
        threshold = {}

    return threshold


# ============================================================
# DQ1 - COMPLETENESS
# ============================================================

def check_completeness(
    df,
    threshold
):
    """
    DQ1 - Completeness

    Metric:
        null_percentage
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        total_cells = (
            total_rows
            * len(df.columns)
        )

        if total_cells == 0:
            return "FAIL"

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

    except Exception:

        return "FAIL"


# ============================================================
# DQ2 - ACCURACY
# ============================================================

def check_accuracy(
    df,
    threshold
):
    """
    DQ2 - Accuracy

    Metric:
        accuracy_failure_percentage

    Current validation:
        NaN values in numeric columns.
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
                "decimal"
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

    except Exception:

        return "FAIL"


# ============================================================
# DQ3 - VALIDITY
# ============================================================

def check_validity(
    df,
    threshold
):
    """
    DQ3 - Validity

    Metric:
        invalid_percentage
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

    except Exception:

        return "FAIL"


# ============================================================
# DQ4 - UNIQUENESS
# ============================================================

def check_uniqueness(
    df,
    threshold
):
    """
    DQ4 - Uniqueness

    Metric:
        duplicate_percentage

    Candidate business keys:
        columns containing 'id' or 'key'
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        candidate_columns = [
            c for c in df.columns
            if (
                    "id" in c.lower()
                    or "key" in c.lower()
            )
        ]

        print(
            f"DQ4 candidate uniqueness columns: {candidate_columns}"
        )

        if not candidate_columns:
            return "PASS"

        duplicate_records = 0

        for column_name in candidate_columns:

            non_null_df = df.filter(
                col(column_name).isNotNull()
            )

            duplicate_rows = (
                non_null_df
                .groupBy(column_name)
                .count()
                .filter(
                    col("count") > 1
                )
            )

            duplicate_count = (
                duplicate_rows
                .selectExpr(
                    "coalesce(sum(count), 0) as total"
                )
                .collect()[0]["total"]
            )

            if duplicate_count is None:
                duplicate_count = 0

            null_count = (
                df.filter(
                    col(column_name).isNull()
                ).count()
            )

            duplicate_records += (
                duplicate_count
                + null_count
            )

        total_possible = (
            total_rows
            * len(candidate_columns)
        )

        failure_percentage = calculate_percentage(
            duplicate_records,
            total_possible
        )

        return get_status(
            failure_percentage,
            threshold
        )

    except Exception:

        return "FAIL"


# ============================================================
# DQ5 - CONSISTENCY
# ============================================================

def check_consistency(
    df,
    threshold
):
    """
    DQ5 - Consistency

    Checks:

        start <= end
        quantity >= 0

    Metric:
        inconsistency_percentage
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

    except Exception:

        return "FAIL"


# ============================================================
# DQ6 - INTEGRITY
# ============================================================

def check_integrity(
    df,
    threshold
):
    """
    DQ6 - Integrity

    Metric:
        integrity_failure_percentage
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

    except Exception:

        return "FAIL"


# ============================================================
# DQ7 - TIMELINESS
# ============================================================

def check_timeliness(
    df,
    threshold
):
    """
    DQ7 - Timeliness

    Current validation:
        future date/timestamp values.

    Metric:
        latency_percentage
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

        future_count = 0

        print(
            f"DQ7 date/timestamp columns: {date_columns}"
        )

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

    except Exception:

        return "FAIL"


# ============================================================
# DQ8 - CONFORMITY
# ============================================================

def check_conformity(
    df,
    threshold
):
    """
    DQ8 - Conformity

    Invalid:
        column names containing spaces.

    Metric:
        non_conforming_percentage
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

    except Exception:

        return "FAIL"


# ============================================================
# DQ9 - RANGE
# ============================================================

def check_range(
    df,
    threshold
):
    """
    DQ9 - Range

    Checks negative values in common
    business numeric fields.

    Metric:
        out_of_range_percentage
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

    except Exception:

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

    Metric:
        duplicate_percentage
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

    except Exception:

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

    Metric:
        null_percentage
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        total_cells = (
            total_rows
            * len(df.columns)
        )

        if total_cells == 0:
            return "FAIL"

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

    except Exception:

        return "FAIL"


# ============================================================
# DQ12 - LENGTH
# ============================================================

def check_length(df, threshold):
    """
    DQ12 - Length

    Checks string columns for values longer than
    the configured maximum length.

    YAML threshold example:

        threshold:
          metric: invalid_length_percentage
          pass: 0
          warning: 5
          fail: 10
          max_length: 1000

    Metric:
        invalid_length_percentage
    """

    try:

        total_rows = df.count()

        if total_rows == 0:
            return "FAIL"

        string_columns = [
            field.name
            for field in df.schema.fields
            if field.dataType.simpleString() == "string"
        ]

        if not string_columns:
            return "PASS"

        # ----------------------------------------------------
        # Get maximum allowed length from YAML
        # ----------------------------------------------------

        max_length = threshold.get(
            "max_length",
            1000
        )

        try:
            max_length = int(max_length)
        except (TypeError, ValueError):
            max_length = 1000

        invalid_values = 0

        for column_name in string_columns:

            invalid_values += (
                df.filter(
                    col(column_name).isNotNull()
                    & (
                        length(col(column_name))
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

    except Exception:
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

    Metric:
        type_mismatch_percentage

    Current generic implementation validates that every
    Spark column has an identifiable data type.
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

    except Exception:

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

    Current validation:
        email columns must contain '@'.

    Metric:
        pattern_failure_percentage
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

    except Exception:

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

    Current validation:
        common financial metrics cannot be negative.

    Metric:
        business_rule_violation_percentage
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

    except Exception:

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

    Metric:
        volume_change_percentage

    If baseline_row_count is available:

        abs(current - baseline)
        ------------------------ * 100
             baseline

    If no baseline exists, the framework cannot calculate
    volume change. A non-empty table is therefore treated
    as PASS for the current implementation.
    """

    try:

        current_row_count = df.count()

        if current_row_count == 0:
            return "FAIL"

        # ----------------------------------------------------
        # No historical baseline
        # ----------------------------------------------------

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
    rules,
    overall_thresholds,
    baseline_row_count=None
):
    """
    Execute all enabled DQ rules.

    YAML controls:

        - enabled/disabled rules
        - rule weights
        - individual thresholds
        - overall score thresholds

    Weighted scoring:

        PASS    = 100% weight
        WARNING = 50% weight
        FAIL    = 0% weight
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

    except Exception:

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

                result[rule["id"]] = "FAIL"

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

    def threshold_for(dq_id):

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

    dq_results["DQ1"] = check_completeness(
        df,
        threshold_for("DQ1")
    )

    dq_results["DQ2"] = check_accuracy(
        df,
        threshold_for("DQ2")
    )

    dq_results["DQ3"] = check_validity(
        df,
        threshold_for("DQ3")
    )

    dq_results["DQ4"] = check_uniqueness(
        df,
        threshold_for("DQ4")
    )

    dq_results["DQ5"] = check_consistency(
        df,
        threshold_for("DQ5")
    )

    dq_results["DQ6"] = check_integrity(
        df,
        threshold_for("DQ6")
    )

    dq_results["DQ7"] = check_timeliness(
        df,
        threshold_for("DQ7")
    )

    dq_results["DQ8"] = check_conformity(
        df,
        threshold_for("DQ8")
    )

    dq_results["DQ9"] = check_range(
        df,
        threshold_for("DQ9")
    )

    dq_results["DQ10"] = check_duplicate(
        df,
        threshold_for("DQ10")
    )

    dq_results["DQ11"] = check_null(
        df,
        threshold_for("DQ11")
    )

    dq_results["DQ12"] = check_length(
        df,
        threshold_for("DQ12")
    )

    dq_results["DQ13"] = check_data_type(
        df,
        threshold_for("DQ13")
    )

    dq_results["DQ14"] = check_pattern(
        df,
        threshold_for("DQ14")
    )

    dq_results["DQ15"] = check_business_rule(
        df,
        threshold_for("DQ15")
    )

    dq_results["DQ16"] = check_volume(
        df,
        threshold_for("DQ16"),
        baseline_row_count
    )

    # ========================================================
    # ONLY ENABLED RULES PARTICIPATE IN SCORE
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
    # TOTAL WEIGHT
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
    # OVERALL STATUS FROM YAML
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

    overall_pass = float(
        overall_thresholds.get(
            "pass",
            90
        )
    )

    overall_warning = float(
        overall_thresholds.get(
            "warning",
            75
        )
    )

    overall_fail = float(
        overall_thresholds.get(
            "fail",
            0
        )
    )

    if total_score >= overall_pass:

        overall_status = "PASS"

    elif total_score >= overall_warning:

        overall_status = "WARNING"

    elif total_score >= overall_fail:

        overall_status = "FAIL"

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

    # --------------------------------------------------------
    # Add only configured rules
    # --------------------------------------------------------

    for rule in rules:

        rule_id = rule["id"]

        if rule.get(
            "enabled",
            False
        ):

            result[rule_id] = dq_results.get(
                rule_id,
                "FAIL"
            )

    result["Total Score"] = total_score

    result["Overall Status"] = overall_status

    return result