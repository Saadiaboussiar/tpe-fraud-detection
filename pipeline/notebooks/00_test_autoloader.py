from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType

src = "/Volumes/tpe_fraud/landing/iot/test/"
chk = "/Volumes/tpe_fraud/landing/checkpoints/test_autoloader"
table = "tpe_fraud.bronze.autoloader_test"

schema = StructType([
    StructField("id", LongType()),
    StructField("txt", StringType()),
])

q = (spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "parquet")
     .schema(schema)
     .load(src)
     .withColumn("_source_file", F.col("_metadata.file_path"))
     .withColumn("_ingested_at", F.current_timestamp())
     .writeStream
     .option("checkpointLocation", chk)
     .trigger(availableNow=True)      # read new files, then stop
     .toTable(table))

q.awaitTermination()
display(spark.table(table))


# COMMAND ----------

dbutils.fs.rm("/Volumes/tpe_fraud/landing/iot/test", True)
dbutils.fs.rm("/Volumes/tpe_fraud/landing/bank/test", True)
dbutils.fs.rm("/Volumes/tpe_fraud/landing/checkpoints/test_autoloader", True)