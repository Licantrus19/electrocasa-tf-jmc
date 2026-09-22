from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, concat_ws


catalogo = spark.conf.get("catalogo")
ruta = spark.conf.get("ruta_empleados")
schema_empleados = spark.conf.get("schema_empleados")


@dp.table(
    name=f"{catalogo}.bronze.empleados",
    comment="Eventos raw de empleados cargados desde CSV"
)
def bronze_empleados():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", schema_empleados)
            .option("rescuedDataColumn", "_rescued_data")
            .load(ruta)
            .withColumn("fec_ingesta", current_timestamp())
            .withColumn("archivo_origen", col("_metadata.file_name"))
            .withColumn(
                "id_lote",
                concat_ws(
                    "_",
                    col("_metadata.file_name"),
                    col("_metadata.file_modification_time").cast("string")
                )
            )
    )