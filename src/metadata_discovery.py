from pyspark.sql import SparkSession


def get_spark_session():
    """
    Get the existing Spark session in Databricks.
    """

    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = SparkSession.builder \
            .appName("Campaign_DQ_Framework") \
            .getOrCreate()

    return spark


def discover_tables(spark, catalog_name):
    """
    Dynamically discover all tables from all schemas
    in the specified catalog.

    information_schema is excluded because it contains
    Databricks system metadata rather than business tables.
    """

    print("\n======================================")
    print("DATABRICKS CATALOG DISCOVERY")
    print("======================================")

    print(f"\nCatalog: {catalog_name}")

    # ---------------------------------------
    # Discover all schemas
    # ---------------------------------------

    schemas_df = spark.sql(
        f"SHOW SCHEMAS IN `{catalog_name}`"
    )

    tables = []

    # ---------------------------------------
    # Loop through every schema
    # ---------------------------------------

    for row in schemas_df.collect():

        schema_name = row["databaseName"]

        # -----------------------------------
        # Skip information_schema
        # -----------------------------------

        if schema_name.lower() == "information_schema":
            continue

        print(f"\nDiscovering schema: {schema_name}")

        # -----------------------------------
        # Discover tables in current schema
        # -----------------------------------

        tables_df = spark.sql(
            f"SHOW TABLES IN "
            f"`{catalog_name}`.`{schema_name}`"
        )

        for table_row in tables_df.collect():

            table_name = table_row["tableName"]

            tables.append(
                (
                    catalog_name,
                    schema_name,
                    table_name
                )
            )

    # ---------------------------------------
    # Print discovered tables
    # ---------------------------------------

    print("\n======================================")
    print("CATALOG / SCHEMA / TABLES")
    print("======================================")

    for catalog, schema, table in tables:

        print(
            f"Catalog: {catalog} | "
            f"Schema: {schema} | "
            f"Table: {table}"
        )

    print("\n======================================")
    print(f"Total Tables Found: {len(tables)}")
    print("======================================")

    return tables