from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').appName('app').getOrCreate()

# spark.conf.set("fs.azure.account.key.guidedcapstonesa.blob.core.windows.net",
#                "8+8uIVJPCcaj2vCkNkG9YtPHZNAerxj9zGiM0VH9OsFxuHLRtvtylaQtB2Ln3enX08DsPD7PT7QFqbFHmNvI7w==")

sc = spark.sparkContext
raw = sc.textFile("wasbs://test@guidedcapstonesa.blob.core.windows.net/part-00000-092ec1db-39ab-4079-9580-f7c7b516a283-c000.txt")
raw.collect()
print(raw.count())