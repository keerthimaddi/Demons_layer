from src.metadata_discovery import (
    get_spark_session,
    discover_tables
)


def main():

    # Get Databricks Spark Session
    spark = get_spark_session()

    # Test catalog
    catalog_name = "wmg"

    print("\n======================================")
    print("DATABRICKS CATALOG DISCOVERY")
    print("======================================")

    print(f"\nCatalog: {catalog_name}")

    # Discover schemas and tables
    tables = discover_tables(
        spark,
        catalog_name
    )

    # Display results
    print("\nCatalog / Schema / Tables")
    print("--------------------------------------")

    for catalog, schema, table in tables:

        print(
            f"Catalog: {catalog} | "
            f"Schema: {schema} | "
            f"Table: {table}"
        )

    print("\n======================================")
    print(f"Total Tables Found: {len(tables)}")
    print("======================================")


if __name__ == "__main__":
    main()