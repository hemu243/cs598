wget http://mirrors.estointernet.in/apache/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz
sudo tar -zxvf spark-3.0.0-bin-hadoop3.2.tgz
sudo yum install java
sudo yum install python3-pip
pip3 install kafka-python

# start-master.sh

# Set below path under ~/.profile or ~/.bash_profile
export SPARK_HOME=/home/ec2-user/spark-3.0.0-bin-hadoop3.2
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=/usr/bin/python3

# spark-submit --master local[4] --conf spark.streaming.backpressure.enabled=true --conf spark.streaming.receiver.maxRate=4000 ./ingest_to_kafka.py input
