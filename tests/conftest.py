from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def spark():
    import pyspark
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    jars_dir = Path(pyspark.__file__).parent / "jars"
    jars_local = any(jars_dir.glob("delta-spark_*.jar"))
    if not jars_local:
        # No pre-placed jars (e.g. CI): let delta-spark pull them from Maven.
        try:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except ImportError:
            pass
    session = builder.getOrCreate()
    yield session
    session.stop()
