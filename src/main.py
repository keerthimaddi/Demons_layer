# from src.metadata_discovery import (
#     get_spark_session,
#     discover_tables
# )
#
# from src.dq_engine import run_dq_checks
#
#
# def main():
#
#     # ---------------------------------------
#     # Get Databricks Spark Session
#     # ---------------------------------------
#     spark = get_spark_session()
#
#     # ---------------------------------------
#     # Catalog to check
#     # ---------------------------------------
#     catalog_name = "wmg"
#
#     print("\n======================================")
#     print("CAMPAIGN DATA QUALITY FRAMEWORK")
#     print("======================================")
#
#     print(f"\nCatalog: {catalog_name}")
#
#     # ---------------------------------------
#     # Discover tables
#     # ---------------------------------------
#     tables = discover_tables(
#         spark,
#         catalog_name
#     )
#
#     print(f"\nTables discovered: {len(tables)}")
#
#     # ---------------------------------------
#     # Run DQ checks
#     # ---------------------------------------
#     results = []
#
#     print("\n======================================")
#     print("RUNNING DATA QUALITY CHECKS")
#     print("======================================")
#
#     for catalog, schema, table in tables:
#
#         print(
#             f"\nChecking: "
#             f"{catalog}.{schema}.{table}"
#         )
#
#         result = run_dq_checks(
#             spark,
#             catalog,
#             schema,
#             table,
#             rules
#         )
#
#         results.append(result)
#
#         print(
#             f"DQ1={result['DQ1']} | "
#             f"DQ2={result['DQ2']} | "
#             f"DQ3={result['DQ3']} | "
#             f"DQ4={result['DQ4']} | "
#             f"DQ5={result['DQ5']} | "
#             f"Score={result['Total Score']}%"
#         )
#
#     # ---------------------------------------
#     # Convert results to Spark DataFrame
#     # ---------------------------------------
#     report_df = spark.createDataFrame(results)
#
#     # ---------------------------------------
#     # Display final report
#     # ---------------------------------------
#     print("\n======================================")
#     print("FINAL DATA QUALITY REPORT")
#     print("======================================")
#
#     report_df.show(
#         truncate=False
#     )
#
#     print(
#         f"\nTotal tables evaluated: "
#         f"{len(results)}"
#     )
#
#
# if __name__ == "__main__":
#     main()

from src.metadata_discovery import (
    get_spark_session,
    discover_tables
)

from src.rule_loader import load_dq_rules

from src.dq_engine import run_dq_checks


def main():

    # ---------------------------------------
    # Get Databricks Spark Session
    # ---------------------------------------
    spark = get_spark_session()

    # ---------------------------------------
    # Catalog to check
    # ---------------------------------------
    catalog_name = "wmg"

    print("\n======================================")
    print("CAMPAIGN DATA QUALITY FRAMEWORK")
    print("======================================")

    print(f"\nCatalog: {catalog_name}")

    # ---------------------------------------
    # Load DQ Rules from YAML
    # ---------------------------------------
    print("\n======================================")
    print("LOADING DQ RULES")
    print("======================================")

    rules = load_dq_rules()

    enabled_rules = [
        rule
        for rule in rules
        if rule.get("enabled", False)
    ]

    total_weight = sum(
        float(rule["default_weight"])
        for rule in enabled_rules
    )

    print(f"\nTotal DQ Rules : {len(rules)}")
    print(f"Enabled DQ Rules: {len(enabled_rules)}")
    print(f"Total Weight    : {total_weight}")

    # ---------------------------------------
    # Discover tables
    # ---------------------------------------
    tables = discover_tables(
        spark,
        catalog_name
    )

    print(f"\nTables discovered: {len(tables)}")

    # ---------------------------------------
    # Run DQ checks
    # ---------------------------------------
    results = []

    print("\n======================================")
    print("RUNNING DATA QUALITY CHECKS")
    print("======================================")

    for catalog, schema, table in tables:

        print(
            f"\nChecking: "
            f"{catalog}.{schema}.{table}"
        )

        result = run_dq_checks(
            spark,
            catalog,
            schema,
            table,
            rules
        )

        results.append(result)

        # -----------------------------------
        # Print all 16 DQ results
        # -----------------------------------
        print(
            f"DQ1={result['DQ1']} | "
            f"DQ2={result['DQ2']} | "
            f"DQ3={result['DQ3']} | "
            f"DQ4={result['DQ4']} | "
            f"DQ5={result['DQ5']} | "
            f"DQ6={result['DQ6']} | "
            f"DQ7={result['DQ7']} | "
            f"DQ8={result['DQ8']} | "
            f"DQ9={result['DQ9']} | "
            f"DQ10={result['DQ10']} | "
            f"DQ11={result['DQ11']} | "
            f"DQ12={result['DQ12']} | "
            f"DQ13={result['DQ13']} | "
            f"DQ14={result['DQ14']} | "
            f"DQ15={result['DQ15']} | "
            f"DQ16={result['DQ16']} | "
            f"Score={result['Total Score']}%"
        )

    # ---------------------------------------
    # Convert results to Spark DataFrame
    # ---------------------------------------
    if results:

        report_df = spark.createDataFrame(results)

        # -----------------------------------
        # Reorder columns
        # -----------------------------------
        report_df = report_df.select(
            "catalog",
            "schema",
            "table",
            "DQ1",
            "DQ2",
            "DQ3",
            "DQ4",
            "DQ5",
            "DQ6",
            "DQ7",
            "DQ8",
            "DQ9",
            "DQ10",
            "DQ11",
            "DQ12",
            "DQ13",
            "DQ14",
            "DQ15",
            "DQ16",
            "Total Score"
        )

        # -----------------------------------
        # Display final report
        # -----------------------------------
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

        print("\nNo tables were evaluated.")

    # ---------------------------------------
    # Stop Spark
    # ---------------------------------------
    spark.stop()


if __name__ == "__main__":
    main()