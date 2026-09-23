from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, lower, when, regexp_replace


catalogo = spark.conf.get("catalogo")


@dp.table(
    name=f"{catalogo}.silver.productos",
    comment="Catalogo de productos limpio y estandarizado"
)
def silver_productos():

    productos = spark.read.table(f"{catalogo}.bronze.productos")

    return (
        productos

        # Limpiar precio y convertir a double
        .withColumn(
            "precio_lista",
            regexp_replace(
                trim(col("precio_lista")),
                "^S/\\s*",
                ""
            ).cast("double")
        )

        # Normalizar categorias
        .withColumn(
            "categoria",
            when(
                lower(trim(col("categoria"))).isin(
                    "climatizacion",
                    "climatización"
                ),
                "climatizacion"
            )
            .when(
                lower(trim(col("categoria"))) == "cocina",
                "cocina"
            )
            .when(
                lower(trim(col("categoria"))).isin(
                    "electronica",
                    "electrónica"
                ),
                "electronica"
            )
            .when(
                lower(trim(col("categoria"))) == "entretenimiento",
                "entretenimiento"
            )
            .when(
                lower(trim(col("categoria"))).isin(
                    "linea blanca",
                    "línea blanca",
                    "linea_blanca"
                ),
                "linea_blanca"
            )
            .otherwise(lower(trim(col("categoria"))))
        )

        # Marca es opcional
        .fillna({
            "marca": "sin_marca"
        })
    )