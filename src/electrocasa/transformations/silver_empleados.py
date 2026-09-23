from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, when, lit
)
from pyspark.sql.window import Window
from pyspark.sql.functions import count


catalogo = spark.conf.get("catalogo")


def limpiar_empleados():
    empleados = spark.read.table(f"{catalogo}.bronze.empleados")

    ventana = Window.partitionBy("id_empleado", "fecha_evento")

    return (
        empleados
        .withColumn("nombre", trim(col("nombre")))
        .withColumn("dni", trim(col("dni")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("salario", col("salario").cast("double"))
        .withColumn("fecha_evento", col("fecha_evento").cast("date"))
        .withColumn(
            "tipo_evento",
            lower(
                regexp_replace(
                    trim(col("tipo_evento")),
                    " ",
                    "_"
                )
            )
        )
        .withColumn(
            "salario_atipico",
            col("salario") > 10000
        )
        .withColumn(
            "eventos_misma_fecha",
            count("*").over(ventana)
        )
    )


@dp.table(
    name=f"{catalogo}.silver.empleados_eventos",
    comment="Eventos de empleados limpios para historizacion"
)
def silver_empleados_eventos():
    return (
        limpiar_empleados()
        .filter(
            col("fecha_evento").isNotNull()
            & (col("eventos_misma_fecha") == 1)
        )
        .drop("eventos_misma_fecha")
    )


@dp.table(
    name=f"{catalogo}.silver.empleados_cuarentena",
    comment="Eventos de empleados sin secuencia temporal confiable"
)
def silver_empleados_cuarentena():
    return (
        limpiar_empleados()
        .filter(
            col("fecha_evento").isNull()
            | (col("eventos_misma_fecha") > 1)
        )
        .withColumn(
            "motivo_rechazo",
            when(
                col("fecha_evento").isNull(),
                lit("fecha_evento_faltante")
            ).otherwise(
                lit("fecha_evento_ambigua")
            )
        )
        .drop("eventos_misma_fecha")
    )