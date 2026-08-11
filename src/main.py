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

    print(
        "\nOverall Score Thresholds:"
    )

    print(
        f"PASS    >= {overall_thresholds.get('pass', 90)}"
    )

    print(
        f"WARNING >= {overall_thresholds.get('warning', 75)}"
    )

    print(
        f"FAIL    < {overall_thresholds.get('warning', 75)}"
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
        # Baseline volume
        #
        # Not available yet.
        # DQ16 will use PASS when the table is non-empty.
        #
        # Later this can come from a historical volume table.
        # ----------------------------------------------------

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
        # Dynamic DQ output
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
    # FINAL REPORT
    # ========================================================

    if results:

        report_df = spark.createDataFrame(
            results
        )

        # ----------------------------------------------------
        # Dynamic DQ columns
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
        # Select columns that actually exist
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
        # Display report
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


if __name__ == "__main__":
    main()