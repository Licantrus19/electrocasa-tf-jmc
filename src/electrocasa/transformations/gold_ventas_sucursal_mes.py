from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, trim, when, date_trunc,
    sum, avg, countDistinct
)


catalogo = spark.conf.get("catalogo")


@dp.table(
    name=f"{catalogo}.gold.ventas_sucursal_mes",
    comment="Ventas y ticket promedio por sucursal y mes"
)
def gold_ventas_sucursal_mes():
    ventas = spark.read.table(
        f"{catalogo}.silver.ventas"
    )

    return (
        ventas
        .withColumn(
            "sucursal_id",
            when(
                col("sucursal_id").isNull()
                | (trim(col("sucursal_id")) == ""),
                "sin_sucursal"
            ).otherwise(trim(col("sucursal_id")))
        )
        .withColumn(
            "mes",
            date_trunc("month", col("fecha_venta"))
        )
        .groupBy(
            "sucursal_id",
            "mes"
        )
        .agg(
            countDistinct("venta_id").alias("cantidad_ventas"),
            sum("monto_total").alias("monto_ventas"),
            avg("monto_total").alias("ticket_promedio")
        )
    )