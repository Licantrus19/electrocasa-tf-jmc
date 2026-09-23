from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, concat_ws


catalogo = spark.conf.get("catalogo")
ruta = spark.conf.get("ruta_resenas")
schema_resenas = spark.conf.get("schema_resenas")


@dp.table(
    name=f"{catalogo}.bronze.resenas",
    comment="Resenas raw cargadas desde archivos JSON"
)
def bronze_resenas():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_resenas)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("multiLine", "true")
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