from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import expr, col

spark = SparkSession \
    .builder \
    .appName("Basic Structured Operations") \
    .getOrCreate()
    
df = spark.read.format("json").load("data/2015-summary.json")

df.printSchema()

#testing manual  schema possible problemas
myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})
])
df = spark.read.format("json").schema(myManualSchema)\
  .load("data/2015-summary.json")

# expr allow us to write a transformation directly in a column
expr("(((someCol + 5) * 200) - 6) < otherCol")

# doing the same thing with columns would be like
(((col("someCol")+ 5)*200 -6)< col("otherCol"))

