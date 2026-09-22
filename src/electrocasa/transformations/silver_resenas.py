from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, trim, when, lit, array,
    transform, struct, to_date
)


catalogo = spark.conf.get("catalogo")


def limpiar_resenas():
    resenas = spark.read.table(
        f"{catalogo}.bronze.resenas"
    )

    productos = (
        spark.read.table(f"{catalogo}.silver.productos")
        .select("producto_id")
        .distinct()
        .withColumn("producto_existe", lit(True))
    )

    return (
        resenas
        .dropDuplicates()
        .withColumn(
            "calificacion",
            col("calificacion").cast("int")
        )
        .withColumn(
            "fecha_resena",
            to_date(col("fecha_resena"), "yyyy-MM-dd")
        )
        .withColumn(
            "comentario",
            when(
                col("comentario").isNull()
                | (trim(col("comentario")) == ""),
                lit(None)
            ).otherwise(trim(col("comentario")))
        )
        .withColumn(
            "tags",
            when(
                col("tags").isNull(),
                array().cast("array<string>")
            ).otherwise(col("tags"))
        )
        .withColumn(
            "respuestas",
            when(
                col("respuestas").isNull(),
                array().cast(
                    "array<struct<autor:string,texto:string>>"
                )
            ).otherwise(
                transform(
                    col("respuestas"),
                    lambda x: struct(
                        when(
                            x["autor"].isNull()
                            | (trim(x["autor"]) == ""),
                            lit("sin_autor")
                        ).otherwise(
                            trim(x["autor"])
                        ).alias("autor"),
                        trim(x["texto"]).alias("texto")
                    )
                )
            )
        )
        .join(
            productos,
            "producto_id",
            "left"
        )
        .withColumn(
            "producto_huerfano",
            col("producto_existe").isNull()
        )
        .drop("producto_existe")
    )


@dp.table(
    name=f"{catalogo}.silver.resenas",
    comment="Resenas limpias y validadas"
)
@dp.expect_or_drop(
    "calificacion_valida",
    "calificacion BETWEEN 1 AND 5"
)
def silver_resenas():
    return limpiar_resenas()


@dp.table(
    name=f"{catalogo}.silver.resenas_cuarentena",
    comment="Resenas rechazadas por calificacion invalida"
)
def resenas_cuarentena():
    return (
        limpiar_resenas()
        .filter(
            col("calificacion").isNull()
            | (col("calificacion") < 1)
            | (col("calificacion") > 5)
        )
        .withColumn(
            "motivo_rechazo",
            when(
                col("calificacion").isNull(),
                lit("calificacion_nula")
            )
            .when(
                col("calificacion") < 1,
                lit("calificacion_menor_1")
            )
            .otherwise(
                lit("calificacion_mayor_5")
            )
        )
    )