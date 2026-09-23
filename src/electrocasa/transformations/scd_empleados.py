from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, expr
)


catalogo = spark.conf.get("catalogo")


@dp.temporary_view(
    name="empleados_cdc",
    comment="Eventos validos de empleados para AUTO CDC"
)
def empleados_cdc():
    empleados = (
        spark.readStream
        .table(f"{catalogo}.bronze.empleados")
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
        .filter(col("fecha_evento").isNotNull())
    )

    ambiguos = (
        spark.read
        .table(f"{catalogo}.silver.empleados_cuarentena")
        .filter(col("motivo_rechazo") == "fecha_evento_ambigua")
        .select("id_empleado", "fecha_evento")
        .distinct()
    )

    return (
        empleados
        .join(
            ambiguos,
            ["id_empleado", "fecha_evento"],
            "left_anti"
        )
    )


dp.create_streaming_table(
    name=f"{catalogo}.silver.empleados_historial",
    comment="Historial SCD tipo 2 de empleados"
)


dp.create_auto_cdc_flow(
    target=f"{catalogo}.silver.empleados_historial",
    source="empleados_cdc",
    keys=["id_empleado"],
    sequence_by=col("fecha_evento"),
    apply_as_deletes=expr("tipo_evento = 'baja'"),
    except_column_list=[
        "tipo_evento",
        "fecha_evento",
        "fec_ingesta",
        "archivo_origen",
        "id_lote",
        "_rescued_data"
    ],
    stored_as_scd_type="2"
)