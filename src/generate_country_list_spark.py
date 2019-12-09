import sys
import pyspark
from pyspark.sql import SparkSession

# Generate a list of countries.
def main(argv):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("csv").option("header", "true").load(
        "file:///home/kjoshi/luigi-demo/luigi-sandbox/datasets/retail/online_retail_II.csv")
    country_list = df.select("Country").distinct()
    country_list.coalesce(1).write.csv("file:///tmp/demo/country_list/")

if __name__ == '__main__':
    sys.exit(main(sys.argv))
