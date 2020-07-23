# https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html
wget https://archive.apache.org/dist/kafka/2.2.1/kafka_2.12-2.2.1.tgz
tar -xzf kafka_2.12-2.2.1.tgz
aws kafka describe-cluster --region us-east-1 --cluster-arn "arn:aws:kafka:us-east-1:584285917640:cluster/kafka-cluster-1/128b7513-1e86-44cd-9c96-68c2671b3d3c-8"

kafka_2.12-2.2.1/bin/kafka-topics.sh bin/kafka-topics.sh --create --zookeeper z-1.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:2181,z-3.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:2181,z-2.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:2181 --replication-factor 2 --partitions 10 --topic input
