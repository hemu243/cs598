# Kafka
## Reading kafka data using command line
kafka_2.12-2.2.1/bin/kafka-console-consumer.sh --topic input --from-beginning --bootstrap-server b-2.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:9092,b-1.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:9092

## Creating topics
kafka_2.12-2.2.1/bin/kafka-topics.sh bin/kafka-topics.sh --create --zookeeper z-1.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:2181,z-3.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:2181,z-2.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:2181 --replication-factor 2 --partitions 3 --topic <topic_name>

## list topics
kafka_2.12-2.2.1/bin/kafka-topics.sh --list --zookeeper z-1.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:2181,z-3.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:2181,z-2.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:2181


## Spark job
## Initial streaming job
spark-submit --master local[4] --conf spark.streaming.backpressure.enabled=true --conf spark.streaming.receiver.maxRate=4000 ./ingest_to_kafka.py input &

## Run spark jon on EMR
spark-submit --master cluster --conf spark.streaming.backpressure.enabled=true --conf spark.streaming.receiver.maxRate=4000 ./ingest_to_kafka.py input &

spark-submit --deploy-mode cluster --master yarn --num-executors 5 --executor-cores 5 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0  <file_path>

### Good pointer about spark-sumit
An alternative to change conf/spark-defaults.conf is to use the –conf prop=value flag. I present both the spark-submit flag and the property name to use in the spark-defaults.conf file and –conf flag.
Example:
–conf spark.yarn.submit.waitAppCompletion=false
