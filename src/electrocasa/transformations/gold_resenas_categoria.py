from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, coalesce, lit, countDistinct,
    sum, when, round
)


catalogo = spark.conf.get("catalogo")


@dp.table(
    name=f"{catalogo}.gold.resenas_categoria",
    comment="Tasa de resenas negativas por categoria de producto"
)
def gold_resenas_categoria():

    resenas = spark.read.table(
        f"{catalogo}.silver.resenas"
    )

    productos = (
        spark.read.table(f"{catalogo}.silver.productos")
        .select(
            "producto_id",
            "categoria"
        )
        .distinct()
    )

    return (
        resenas
        .join(
            productos,
            "producto_id",
            "left"
        )
        .withColumn(
            "categoria",
            coalesce(col("categoria"), lit("sin_categoria"))
        )
        .groupBy("categoria")
        .agg(
            countDistinct("resena_id").alias("cantidad_resenas"),
            sum(
                when(
                    col("calificacion") <= 2,
                    1
                ).otherwise(0)
            ).alias("resenas_negativas")
        )
        .withColumn(
            "tasa_resenas_negativas",
            round(
                col("resenas_negativas")
                / col("cantidad_resenas")
                * 100,
                2
            )
        )
    )