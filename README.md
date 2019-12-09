## Installation

### Install Luigi
`pip install luigi`

#### Start luigi daemon

./luigid/start_lugid.sh

View output on `localhost:8082`


### Install and Configure Spark
`wget http://mirrors.ocf.berkeley.edu/apache/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz`
`tar -xzvf spark-2.4.4-bin-hadoop2.7.tgz`

#### Start a local master.
`./spark-2.4.4-bin-hadoop2.7/sbin/start-master.sh`

#### Start a local worker.
`./spark-2.4.4-bin-hadoop2.7/sbin/start-slave.sh 0.0.0.0:7077`


#### Issue a verification test.
We'll run the bundled Pi program to verify our installation:
```
./spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
    --class org.apache.spark.examples.SparkPi \
    --master local \
    spark-2.4.4-bin-hadoop2.7/examples/jars/spark-examples_2.11-2.4.4.jar \
    100
```

This should spit out `Pi is roughly 3.1416951141695115` when succesful.
