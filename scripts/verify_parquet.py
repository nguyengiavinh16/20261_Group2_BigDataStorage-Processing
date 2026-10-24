from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("verify-paysim-parquet")
    .getOrCreate()
)

try:
    df = spark.read.parquet("data/processed/paysim_parquet")

    print("Số dòng:", df.count())
    print("Số cột:", len(df.columns))
    print("Tên cột:", df.columns)

    df.printSchema()
    df.show(5, truncate=False)
finally:
    spark.stop()