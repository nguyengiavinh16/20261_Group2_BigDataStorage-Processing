from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("hello-spark").master("local[*]").getOrCreate()
df = spark.createDataFrame(
    [("TRANSFER", 181.0, 1), ("PAYMENT", 9839.64, 0), ("CASH_OUT", 181.0, 1)],
    ["type", "amount", "isFraud"],
)
df.groupBy("isFraud").count().show()
print("Spark version:", spark.version)
spark.stop()
