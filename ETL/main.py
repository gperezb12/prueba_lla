from pyspark.sql import SparkSession
import os
from utils import asignar_grids, calculate_distances, add_event_counts
os.environ["JAVA_HOME"] = 'C:\\Program Files\\Java\\jdk-17'  #para correr pyspark local necesitamos tener java

spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()
print(spark)

from pyspark.sql.functions import col, lit, count, abs, first, round, sum, collect_list, floor,struct, sqrt, pow, array, expr,current_date
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import DataFrame

geo = spark.read.parquet("C:\\Users\\greg2\\Documents\\prueba_lla\\ETL\\data_sample\\geo\\part-00000-887b5b44-c16f-497f-b076-0b5f3e157afb-c000.snappy.parquet", header=True)
labels = spark.read.parquet("C:\\Users\\greg2\\Documents\\prueba_lla\\ETL\\data_sample\\labels\\part-00000-13b2d3ca-4b0e-4079-a7f6-c6f846c69302-c000.snappy.parquet", header=True)

#Join the two dfs 
merged_df = geo.join(labels, on="ID", how="left")

cleaned_df = merged_df.groupBy("ID").agg(
    first("comuna").alias("comuna"),
    first("latitud").alias("latitud"),
    first("longitud").alias("longitud"),
    collect_list("event").alias("event")  # Guarda una lista de todos los eventos que tiene un cliente
)

grid_df = asignar_grids(cleaned_df) #Asignarle una grilla a caad punto, para poder medir los puntos que solo se encuentren en grillas adjacentes

dist_df = calculate_distances(grid_df) #Encuentra la distancia slo entre los puntos en las grillas adyacentes

final_df = add_event_counts(dist_df)


# Guardar el primer DataFrame en S3 como perequet

# Crear un nuevo DataFrame con solo las columnas requeridas para los analistas
dist_simplified_df = final_df.select(
    col("ID_geo"),
    col("comuna"),
    col("ID_event"),
    col("distance"),
    col("date_processed"),
    col("number_of_type1_events"),
    col("number_of_type2_events")
)

# Guardar el segundo DataFrame como perequet en S3



final_df.show(10)

