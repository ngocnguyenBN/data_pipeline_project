from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, when
from pyspark.sql import functions as f

spark = SparkSession.builder.master("local[*]").appName("Us_stock_price").getOrCreate()


data = spark.read.csv(
    "D:/python_r_DA/data/stocks_price_final.csv", sep=",", header=True
)


data.printSchema()


data_schema = [
    StructField("_c0", IntegerType(), True),
    StructField("symbol", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("adjusted", DoubleType(), True),
    StructField("market.cap", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("exchange", StringType(), True),
]

final_struc = StructType(fields=data_schema)
data = spark.read.csv(
    "D:/python_r_DA/data/stocks_price_final.csv",
    sep=",",
    header=True,
    schema=final_struc,
)
data.show()

data = data.withColumnRenamed("market.cap", "market_cap")
# describde data
data.describe().show()
# fill missing data
data = data.na.fill(0)
# filter
data.filter((col("high") >= lit("50")) & (col("low") <= lit("60"))).show(5)

# filter with WHEN
data.select("open", "close", f.when(data.adjusted >= 596300.0, 1).otherwise(0)).show()

# rename column when filter
data.select(
    "open",
    "close",
    when(data.adjusted >= 596300.0, 1).otherwise(0).alias("adjusted_flag"),
).show(5)

# group_by
data.select(["industry", "open", "close", "adjusted"]).groupBy("industry").mean().show()
