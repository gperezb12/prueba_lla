from pyspark.sql.functions import col, lit, count, abs, first, round, sum, collect_list, floor,struct, sqrt, pow, array, expr,current_date
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import DataFrame

def asignar_grids(df: DataFrame, grid_size: int = 50) -> DataFrame:
    """
    Asigna una cuadrícula (grid_x, grid_y) de tamaño `grid_size` a cada punto en el DataFrame de PySpark.

    :param df: DataFrame de PySpark con las columnas 'longitud' y 'latitud'
    :param grid_size: Tamaño del grid en metros (por defecto 50m)
    :return: DataFrame con la nueva columna 'grid' que contiene las coordenadas del grid (x, y)
    """
    df = df.withColumn("grid_x", floor(col("longitud") / grid_size) + 1)
    df = df.withColumn("grid_y", floor(col("latitud") / grid_size) + 1)
    df = df.withColumn("grid", struct(col("grid_x").cast("int"), col("grid_y").cast("int")))

    return df.drop("grid_x", "grid_y")  # Elimina columnas auxiliares si no son necesarias


def calculate_distances(grid_df: DataFrame) -> DataFrame:
    """
    Calcula la distancia entre clientes en grid_df a clientes con eventos en grid_df en un radio de 50 metros,
    considerando solo los puntos dentro del mismo grid o en grids adyacentes.
    También elimina filas donde ID_geo == ID_event (mismo cliente).

    :param grid_df: DataFrame con las columnas ['ID', 'comuna', 'latitud', 'longitud', 'event', 'grid']
    :return: DataFrame con las columnas ['ID_geo', 'comuna', 'latitude_geo', 'longitude_geo',
                                         'ID_event', 'latitude_event', 'longitude_event', 'event', 'distance']
    """

    # Extraer las coordenadas del grid como columnas separadas con sus nombres reales
    grid_df = grid_df.withColumn("grid_x", col("grid.col1")) \
                     .withColumn("grid_y", col("grid.col2"))

    # Filtrar clientes que tienen eventos
    events_df = grid_df.filter(col("event").isNotNull() & (col("event") != array()))

    # Hacer un self-join en la misma comuna y solo en grids cercanos
    distance_df = grid_df.alias("g1").join(
        events_df.alias("g2"),
        (col("g1.comuna") == col("g2.comuna")) &  # Solo en la misma comuna
        (
            (col("g1.grid_x") == col("g2.grid_x")) & (abs(col("g1.grid_y") - col("g2.grid_y")) <= 1) |
            (col("g1.grid_y") == col("g2.grid_y")) & (abs(col("g1.grid_x") - col("g2.grid_x")) <= 1) |
            (abs(col("g1.grid_x") - col("g2.grid_x")) == 1) & (abs(col("g1.grid_y") - col("g2.grid_y")) == 1)
        ),
        "inner"
    ).filter(col("g1.ID") != col("g2.ID"))  # Eliminar filas donde ID_geo == ID_event

    # Calcular la distancia Euclidiana
    distance_df = distance_df.withColumn(
        "distance",
        sqrt(
            pow(col("g1.latitud") - col("g2.latitud"), 2) +
            pow(col("g1.longitud") - col("g2.longitud"), 2)
        )
    ).filter(col("distance") <= 50)  # Filtrar solo los clientes en un radio de 50m

    # Seleccionar las columnas finales
    distance_df = distance_df.select(
        col("g1.ID").alias("ID_geo"),
        col("g1.comuna"),
        col("g1.latitud").alias("latitude_geo"),
        col("g1.longitud").alias("longitude_geo"),
        col("g2.ID").alias("ID_event"),
        col("g2.latitud").alias("latitude_event"),
        col("g2.longitud").alias("longitude_event"),
        col("g2.event"),
        col("distance")
    )

    return distance_df.withColumn("date_processed", current_date()) #Agregar fecha para mantener track semanal

