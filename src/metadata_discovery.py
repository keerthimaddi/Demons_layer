from pyspark.sql import SparkSession


def get_spark_session():
    """
    Get the existing Spark session.

    In Databricks, the Spark session is already created.
    We reuse that session so that Unity Catalog is available.
    """

    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = (
            SparkSession.builder
            .appName("Campaign_DQ_Framework")
            .getOrCreate()
        )

    return spark


def discover_tables(spark, catalog_name):
    """
    Dynamically discover all tables from all schemas
    in the specified Unity Catalog catalog.

    information_schema is excluded because it contains
    Databricks system metadata rather than business tables.
    """

    print("\n======================================")
    print("DATABRICKS CATALOG DISCOVERY")
    print("======================================")

    print(f"\nCatalog: {catalog_name}")

    # ========================================================
    # VALIDATE CATALOG
    # ========================================================

    print(
        f"\nChecking catalog: {catalog_name}"
    )

    try:

        catalog_df = spark.sql(
            "SHOW CATALOGS"
        )

        catalogs = [
            row["catalog"]
            for row in catalog_df.collect()
        ]

    except Exception as error:

        raise RuntimeError(
            "Unable to access Databricks catalogs. "
            "Make sure this code is being executed "
            "inside Databricks with Unity Catalog access."
        ) from error

    if catalog_name not in catalogs:

        raise ValueError(
            f"Catalog '{catalog_name}' was not found "
            f"in the current Databricks environment.\n"
            f"Available catalogs: {catalogs}"
        )

    print(
        f"Catalog '{catalog_name}' found."
    )

    # ========================================================
    # DISCOVER SCHEMAS
    # ========================================================

    schemas_df = spark.sql(
        f"SHOW SCHEMAS IN `{catalog_name}`"
    )

    tables = []

    # ========================================================
    # LOOP THROUGH SCHEMAS
    # ========================================================

    for row in schemas_df.collect():

        # Databricks Runtime normally returns
        # databaseName for SHOW SCHEMAS.

        schema_name = row["databaseName"]

        # ----------------------------------------------------
        # Skip information_schema
        # ----------------------------------------------------

        if schema_name.lower() == "information_schema":
            continue

        print(
            f"\nDiscovering schema: {schema_name}"
        )

        # ====================================================
        # DISCOVER TABLES
        # ====================================================

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

    # ========================================================
    # PRINT DISCOVERED TABLES
    # ========================================================

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
    print(
        f"Total Tables Found: {len(tables)}"
    )
    print("======================================")

    return tables