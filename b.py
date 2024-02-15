from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Création de la session Spark
spark = SparkSession.builder \
    .appName("Exemple Lecture CSV et Transformation RDD") \
    .getOrCreate()

# Définition du schéma des données
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Création du DataFrame à partir des données en mémoire
data = [
    (1, "John", 30),
    (2, "Alice", 25),
    (3, "Bob", 35),
    (4, "Emma", 28),
    (5, "Michael", 40)
]
df = spark.createDataFrame(data, schema)

# Conversion du DataFrame en RDD
rdd = df.rdd

# Transformation RDD avec map()
rdd_transformed = rdd.map(lambda x: (x[0], x[1], x[2] * 2))  # Exemple de transformation (en multipliant l'âge par 2)

# Action RDD pour collecter les résultats
resultats = rdd_transformed.collect()

# Affichage des résultats
for resultat in resultats:
    print(resultat)

# Fermeture de la session Spark
spark.stop()
