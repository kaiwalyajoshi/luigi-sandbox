import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col, regexp_extract

# Generate a list of countries.
def main(argv):
    spark = SparkSession.builder.getOrCreate()

    country = argv[2]

    df = spark.read.format("csv").option("header", "true").load(
        "file:///home/kjoshi/luigi-demo/luigi-sandbox/datasets/retail/online_retail_II.csv")

    country_data = df.filter("Country='{}'".format(country))
    country_data.coalesce(1).write.option("header", "true").csv("file:///tmp/demo/{}/raw/".format(country))

if __name__ == '__main__':
    sys.exit(main(sys.argv))