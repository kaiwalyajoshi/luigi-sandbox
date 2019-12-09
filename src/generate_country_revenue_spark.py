import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col, regexp_extract

# Generate a list of countries.
def main(argv):
    spark = SparkSession.builder.getOrCreate()

    country = argv[2]
    read_path = "file:///tmp/demo/{}/raw/".format(country)

    df = spark.read.format("csv").option("header", "true").load(read_path)
    sales = df.withColumn("Sale", col("Quantity")*col("Price"))
    dateOfPurchase = sales.withColumn("Date", col("InvoiceDate").substr(0, 10))
    result = dateOfPurchase.groupBy("Country", "Date").sum("Sale")
    result.coalesce(1).write.option("header", "true").csv("file:///tmp/demo/{}/revenue/".format(country))

if __name__ == '__main__':
    sys.exit(main(sys.argv))