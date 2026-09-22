from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, trim, when, lit, to_date
)


catalogo = spark.conf.get("catalogo")


def limpiar_devoluciones():
    devoluciones = spark.read.table(
        f"{catalogo}.bronze.devoluciones"
    )

    productos = (
        spark.read.table(f"{catalogo}.silver.productos")
        .select("producto_id")
        .distinct()
        .withColumn("producto_existe", lit(True))
    )

    return (
        devoluciones
        .dropDuplicates([
            "devolucion_id",
            "pedido_id",
            "sucursal_id",
            "producto_id",
            "motivo",
            "monto_reembolso",
            "fecha_devolucion"
        ])
        .withColumn(
            "monto_reembolso",
            col("monto_reembolso").cast("double")
        )
        .withColumn(
            "fecha_devolucion",
            to_date(col("fecha_devolucion"), "yyyy-MM-dd")
        )
        .withColumn(
            "motivo",
            when(
                col("motivo").isNull()
                | (trim(col("motivo")) == ""),
                lit(None)
            ).otherwise(trim(col("motivo")))
        )
        .withColumn(
            "pedido_faltante",
            col("pedido_id").isNull()
            | (trim(col("pedido_id")) == "")
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
    name=f"{catalogo}.silver.devoluciones",
    comment="Devoluciones limpias y validadas"
)
@dp.expect_or_drop(
    "monto_reembolso_valido",
    "monto_reembolso >= 0"
)
def silver_devoluciones():
    return limpiar_devoluciones()


@dp.table(
    name=f"{catalogo}.silver.devoluciones_cuarentena",
    comment="Devoluciones rechazadas por monto de reembolso invalido"
)
def devoluciones_cuarentena():
    return (
        limpiar_devoluciones()
        .filter(
            col("monto_reembolso").isNull()
            | (col("monto_reembolso") < 0)
        )
        .withColumn(
            "motivo_rechazo",
            when(
                col("monto_reembolso").isNull(),
                lit("monto_reembolso_nulo")
            ).otherwise(
                lit("monto_reembolso_negativo")
            )
        )
    )