"""Schema Spark cho dataset Sparkov (fraudTrain.csv / fraudTest.csv).

Thứ tự cột lấy theo mô tả dataset; hãy đối chiếu với dòng header của file
thực tế sau khi tải về (cột đầu tiên là chỉ số dòng, không có tên).
"""
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

RAW_TRANSACTION_SCHEMA = StructType(
    [
        StructField("row_id", IntegerType(), True),  # cột index không tên trong CSV
        StructField("trans_date_trans_time", StringType(), True),  # 'YYYY-MM-DD HH:MM:SS'
        StructField("cc_num", LongType(), True),
        StructField("merchant", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amt", DoubleType(), True),
        StructField("first", StringType(), True),
        StructField("last", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("long", DoubleType(), True),
        StructField("city_pop", IntegerType(), True),
        StructField("job", StringType(), True),
        StructField("dob", StringType(), True),  # 'YYYY-MM-DD'
        StructField("trans_num", StringType(), True),
        StructField("unix_time", LongType(), True),
        StructField("merch_lat", DoubleType(), True),
        StructField("merch_long", DoubleType(), True),
        StructField("is_fraud", IntegerType(), True),
    ]
)

# Cột định danh cá nhân giả / không dùng làm feature cho mô hình
DROP_COLUMNS = ["row_id", "first", "last", "street", "dob", "trans_num"]

LABEL_COL = "is_fraud"
ENTITY_COL = "cc_num"
EVENT_TIME_COL = "trans_date_trans_time"


def read_raw(spark, path):
    """Đọc CSV thô với schema cố định."""
    return (
        spark.read.option("header", True)
        .schema(RAW_TRANSACTION_SCHEMA)
        .csv(path)
    )
