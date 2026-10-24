from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("spark-compatibility-test")
    .getOrCreate()
)

try:
    spark.range(5).show()
    print("Spark version:", spark.version)
finally:
    spark.stop()