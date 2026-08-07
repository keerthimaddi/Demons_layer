from src.metadata_discovery import (
    get_spark_session,
    discover_tables
)

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
            table
        )

        results.append(result)

        print(
            f"DQ1={result['DQ1']} | "
            f"DQ2={result['DQ2']} | "
            f"DQ3={result['DQ3']} | "
            f"DQ4={result['DQ4']} | "
            f"DQ5={result['DQ5']} | "
            f"Score={result['Total Score']}%"
        )

    # ---------------------------------------
    # Convert results to Spark DataFrame
    # ---------------------------------------
    report_df = spark.createDataFrame(results)

    # ---------------------------------------
    # Display final report
    # ---------------------------------------
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


if __name__ == "__main__":
    main()