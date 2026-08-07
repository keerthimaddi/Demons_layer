from pyspark.sql import SparkSession


def get_spark_session():
    """
    Get the Spark session provided by Databricks.
    """

    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError(
            "No active Spark session found. "
            "This code must be executed inside Databricks."
        )

    return spark


def get_schemas(spark, catalog_name):
    """
    Get all schemas from a catalog.
    """

    return (
        spark.sql(f"SHOW SCHEMAS IN `{catalog_name}`")
        .select("namespace")
    )


def get_tables(spark, catalog_name, schema_name):
    """
    Get all tables from a specific catalog and schema.
    """

    return (
        spark.sql(
            f"SHOW TABLES IN `{catalog_name}`.`{schema_name}`"
        )
        .select("database", "tableName", "isTemporary")
    )


def discover_tables(spark, catalog_name):
    """
    Discover all schemas and tables inside a catalog.
    """

    schemas_df = get_schemas(spark, catalog_name)

    results = []

    for row in schemas_df.collect():

        schema_name = row["namespace"]

        tables_df = get_tables(
            spark,
            catalog_name,
            schema_name
        )

        for table_row in tables_df.collect():

            results.append(
                (
                    catalog_name,
                    schema_name,
                    table_row["tableName"]
                )
            )

    return results