from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, trim, when, countDistinct
)


catalogo = spark.conf.get("catalogo")


@dp.table(
    name=f"{catalogo}.gold.dotacion_sucursal",
    comment="Dotacion activa de empleados por sucursal"
)
def gold_dotacion_sucursal():

    empleados = spark.read.table(
        f"{catalogo}.silver.empleados_historial"
    )

    return (
        empleados
        .filter(
            col("__END_AT").isNull()
        )
        .withColumn(
            "sucursal_id",
            when(
                col("sucursal_id").isNull()
                | (trim(col("sucursal_id")) == ""),
                "sin_sucursal"
            ).otherwise(trim(col("sucursal_id")))
        )
        .groupBy("sucursal_id")
        .agg(
            countDistinct("id_empleado").alias("empleados_activos")
        )
    )