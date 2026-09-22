from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    when,
    coalesce,
    to_date,
    current_timestamp,
    lit
)


catalogo = spark.conf.get("catalogo")


def limpiar_ventas():

    ventas = spark.read.table(f"{catalogo}.bronze.ventas")

    return (
        ventas

        # Eliminar reingestas identicas
        .dropDuplicates([
            "venta_id",
            "sucursal_id",
            "producto_id",
            "cantidad",
            "monto_total",
            "metodo_pago",
            "fecha_venta",
            "canal"
        ])

        # Tipos Silver
        .withColumn("cantidad", col("cantidad").cast("int"))
        .withColumn("monto_total", col("monto_total").cast("double"))

        # Unificar formatos de fecha
        .withColumn(
            "fecha_venta",
            coalesce(
                to_date(col("fecha_venta"), "yyyy-MM-dd"),
                to_date(col("fecha_venta"), "dd/MM/yyyy")
            )
        )

        # Normalizar metodo de pago
        .withColumn(
            "metodo_pago",
            when(
                lower(trim(col("metodo_pago"))).isin("efectivo", "efv"),
                "efectivo"
            )
            .when(
                lower(trim(col("metodo_pago"))).isin(
                    "tarjeta de credito",
                    "tarjeta_credito",
                    "tarjeta",
                    "tc"
                ),
                "tarjeta_credito"
            )
            .when(
                lower(trim(col("metodo_pago"))).isin(
                    "transferencia",
                    "transferencia bancaria"
                ),
                "transferencia"
            )
            .when(
                lower(trim(col("metodo_pago"))) == "plin",
                "plin"
            )
            .when(
                lower(trim(col("metodo_pago"))) == "yape",
                "yape"
            )
            .otherwise(lower(trim(col("metodo_pago"))))
        )
    )


@dp.table(
    name=f"{catalogo}.silver.ventas",
    comment="Ventas limpias y validadas"
)
@dp.expect_or_drop(
    "monto_total_valido",
    "monto_total > 0"
)
def silver_ventas():
    return limpiar_ventas()


@dp.table(
    name=f"{catalogo}.silver.ventas_cuarentena",
    comment="Ventas rechazadas por reglas de calidad"
)
def ventas_cuarentena():

    return (
        limpiar_ventas()
        .filter(
            col("monto_total").isNull() |
            (col("monto_total") <= 0)
        )
        .withColumn(
            "motivo_rechazo",
            when(
                col("monto_total").isNull(),
                lit("monto_total_nulo")
            ).otherwise(
                lit("monto_total_no_positivo")
            )
        )
        .withColumn(
            "fec_rechazo",
            current_timestamp()
        )
    )