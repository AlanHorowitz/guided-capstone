from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').appName('app').getOrCreate()


sc = spark.sparkContext
raw = sc.textFile("wasbs://test@guidedcapstonesa.blob.core.windows.net/part-00000-092ec1db-39ab-4079-9580-f7c7b516a283-c000.txt")
raw.collect()
print(raw.count())