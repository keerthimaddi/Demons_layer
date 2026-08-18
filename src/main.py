from src.metadata_discovery import (
    get_spark_session,
    discover_tables
)

from src.rule_loader import load_dq_rules

from src.dq_engine import run_dq_checks


def main():

    # ========================================================
    # GET SPARK SESSION
    # ========================================================

    spark = get_spark_session()

    # ========================================================
    # CATALOG
    # ========================================================

    catalog_name = "wmg"

    print("\n======================================")
    print("CAMPAIGN DATA QUALITY FRAMEWORK")
    print("======================================")

    print(
        f"\nCatalog: {catalog_name}"
    )

    # ========================================================
    # LOAD DQ RULES
    # ========================================================

    print("\n======================================")
    print("LOADING DQ RULES")
    print("======================================")

    rules, overall_thresholds = load_dq_rules()

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

    total_weight = sum(
        float(
            rule.get(
                "default_weight",
                0
            )
        )
        for rule in enabled_rules
    )

    print(
        f"\nTotal DQ Rules    : {len(rules)}"
    )

    print(
        f"Enabled DQ Rules  : {len(enabled_rules)}"
    )

    print(
        f"Total Weight      : {total_weight}"
    )

    # ========================================================
    # OVERALL SCORE THRESHOLDS
    # ========================================================

    print(
        "\nOverall Score Thresholds:"
    )

    print(
        f"PASS    >= "
        f"{overall_thresholds.get('pass', 90)}"
    )

    print(
        f"WARNING >= "
        f"{overall_thresholds.get('warning', 75)}"
    )

    print(
        f"FAIL    < "
        f"{overall_thresholds.get('warning', 75)}"
    )

    # ========================================================
    # DISCOVER TABLES
    # ========================================================

    print("\n======================================")
    print("DATABRICKS CATALOG DISCOVERY")
    print("======================================")

    tables = discover_tables(
        spark,
        catalog_name
    )

    print(
        f"\nTables discovered: {len(tables)}"
    )

    # ========================================================
    # RUN DQ CHECKS
    # ========================================================

    results = []

    print("\n======================================")
    print("RUNNING DATA QUALITY CHECKS")
    print("======================================")

    for catalog, schema, table in tables:

        print(
            f"\nChecking: "
            f"{catalog}.{schema}.{table}"
        )

        # ----------------------------------------------------
        # TEMPORARY VOLUME BASELINE
        # ----------------------------------------------------
        #
        # DQ16 will be properly implemented later.
        # For now, keep the existing behavior.
        #

        baseline_row_count = None

        result = run_dq_checks(
            spark,
            catalog,
            schema,
            table,
            rules,
            overall_thresholds,
            baseline_row_count
        )

        results.append(
            result
        )

        # ----------------------------------------------------
        # DQ RESULT OUTPUT
        # ----------------------------------------------------

        dq_output = " | ".join(
            f"{rule['id']}="
            f"{result.get(rule['id'], 'N/A')}"
            for rule in rules
            if rule.get(
                "enabled",
                False
            )
        )

        print(
            f"{dq_output} | "
            f"Score={result['Total Score']}% | "
            f"Overall={result['Overall Status']}"
        )

    # ========================================================
    # FINAL DATA QUALITY REPORT
    # ========================================================

    if results:

        report_df = spark.createDataFrame(
            results
        )

        # ----------------------------------------------------
        # DQ COLUMNS
        # ----------------------------------------------------

        dq_columns = [
            rule["id"]
            for rule in rules
            if rule.get(
                "enabled",
                False
            )
        ]

        report_columns = [
            "catalog",
            "schema",
            "table"
        ] + dq_columns + [
            "Total Score",
            "Overall Status"
        ]

        # ----------------------------------------------------
        # SELECT AVAILABLE COLUMNS
        # ----------------------------------------------------

        available_columns = [
            column
            for column in report_columns
            if column in report_df.columns
        ]

        report_df = report_df.select(
            *available_columns
        )

        # ----------------------------------------------------
        # DISPLAY FINAL REPORT
        # ----------------------------------------------------

        print("\n======================================")
        print("FINAL DATA QUALITY REPORT")
        print("======================================")

        report_df.show(
            truncate=False
        )

        print(
            f"\nTotal tables evaluated: "
            f"{len(results)}"
        )

    else:

        print(
            "\nNo tables were evaluated."
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()