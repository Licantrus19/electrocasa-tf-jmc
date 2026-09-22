from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, lower, when


catalogo = spark.conf.get("catalogo")


@dp.table(
    name=f"{catalogo}.silver.tracking_envios",
    comment="Tracking de envios limpio y estandarizado"
)
def silver_tracking_envios():
    tracking = spark.read.table(
        f"{catalogo}.bronze.tracking_envios"
    )

    return (
        tracking
        .dropDuplicates([
            "tracking_id",
            "pedido_id",
            "courier",
            "estado_entrega",
            "sucursal_origen",
            "fecha_actualizacion"
        ])
        .withColumn(
            "courier",
            lower(trim(col("courier")))
        )
        .withColumn(
            "estado_entrega",
            when(
                lower(trim(col("estado_entrega"))).isin(
                    "en camino",
                    "en_camino",
                    "en_transito"
                ),
                "en_transito"
            )
            .when(
                lower(trim(col("estado_entrega"))) == "entregado",
                "entregado"
            )
            .when(
                lower(trim(col("estado_entrega"))) == "devuelto",
                "devuelto"
            )
            .when(
                lower(trim(col("estado_entrega"))) == "pendiente",
                "pendiente"
            )
            .otherwise(lower(trim(col("estado_entrega"))))
        )
    )