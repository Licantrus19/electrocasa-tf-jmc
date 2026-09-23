from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, sum, countDistinct, coalesce, lit,
    row_number, desc
)
from pyspark.sql.window import Window


catalogo = spark.conf.get("catalogo")


@dp.table(
    name=f"{catalogo}.gold.productos_ventas_devoluciones",
    comment="Ranking de productos vendidos y devueltos"
)
def gold_productos_ventas_devoluciones():

    ventas = (
        spark.read.table(f"{catalogo}.silver.ventas")
        .groupBy("producto_id")
        .agg(
            sum("cantidad").alias("unidades_vendidas"),
            countDistinct("venta_id").alias("cantidad_ventas")
        )
    )

    devoluciones = (
        spark.read.table(f"{catalogo}.silver.devoluciones")
        .groupBy("producto_id")
        .agg(
            countDistinct("devolucion_id").alias("cantidad_devoluciones"),
            sum("monto_reembolso").alias("monto_reembolsado")
        )
    )

    productos = (
        spark.read.table(f"{catalogo}.silver.productos")
        .select(
            "producto_id",
            "nombre_producto",
            "categoria"
        )
        .distinct()
    )

    ranking_ventas = Window.orderBy(
        desc("unidades_vendidas")
    )

    ranking_devoluciones = Window.orderBy(
        desc("cantidad_devoluciones")
    )

    return (
        ventas
        .join(
            devoluciones,
            "producto_id",
            "full"
        )
        .join(
            productos,
            "producto_id",
            "left"
        )
        .withColumn(
            "unidades_vendidas",
            coalesce(col("unidades_vendidas"), lit(0))
        )
        .withColumn(
            "cantidad_ventas",
            coalesce(col("cantidad_ventas"), lit(0))
        )
        .withColumn(
            "cantidad_devoluciones",
            coalesce(col("cantidad_devoluciones"), lit(0))
        )
        .withColumn(
            "monto_reembolsado",
            coalesce(col("monto_reembolsado"), lit(0.0))
        )
        .withColumn(
            "ranking_ventas",
            row_number().over(ranking_ventas)
        )
        .withColumn(
            "ranking_devoluciones",
            row_number().over(ranking_devoluciones)
        )
    )