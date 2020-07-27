#!/usr/bin/python
from pyspark import SparkContext
# from pyspark.streaming import StreamingContext
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import sys


def printResults(rdd):
    """
    Print partial results to screen.
    """
    print("----------------- SNAPSHOT ----------------------")
    for line in rdd.take(10):
        print(line)
    print("SIZE: %d" % rdd.count())


def saveResults(rdd):
    """
	Save results as a report.
	"""
    results = rdd.collect();
    if len(results) == 0:
        return
    file = open(sys.argv[2], 'w')
    for item in results:
        file.write("\n%s -> %s: %s\n\n" % (item[0][0], item[0][1], item[1]))


def updateFunction(newValues, runningAvg):
    if runningAvg is None:
        runningAvg = (0.0, 0, 0.0)
    # calculate sum, count and average.
    prod = sum(newValues, runningAvg[0])
    count = runningAvg[1] + len(newValues)
    avg = prod / float(count)
    return (prod, count, avg)


def sendToKafka(records):
    """
	Send records to Kafka. The format is the following
	JFK LAX -3.013333
	JFK ORD 0.01113
	"""
    producer = KafkaProducer(
        bootstrap_servers='b-2.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:9092,b-1.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:9092')
    for record in records:
        message = "%s %s %s" % (record[0][0], record[0][1], record[1])
        producer.send_messages('airports_airports_arrival', message.encode())


# MAIN

sc = SparkSession.builder.appName("AirportAirportArrival").getOrCreate()
# sc = SparkContext(appName="AirportAirportArrival")
# sc.setLevel('ERROR')

# Create a local StreamingContext
# ssc = StreamingContext(sc, 1)
# ssc.checkpoint("s3a://hsc4-cc-part2-streaming/checkpoints/checkpoint-airport-airport-arrival")

lines = sc.readStream.format("kafka").option("kafka.bootstrap.servers",
                                               "b-2.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:9092,b-1.kafka-cluster-1.rp7oyu.c8.kafka.us-east-1.amazonaws.com:9092") \
    .option("subscribe", "input").load()

# lines = KafkaUtils.createDirectStream(ssc, ['input'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Split each line by separator
lines = lines.map(lambda tup: tup[1])
rows = lines.map(lambda line: line.split())

# Get the airports
rows = rows.filter(lambda row: len(row) > 8)
airports_fromto = rows.map(lambda row: ((row[0], row[1]), float(row[8])))

# Count averages
airports_fromto = airports_fromto.updateStateByKey(updateFunction)
# Change key to just airports
airports = airports_fromto.map(lambda row: ((row[0][0], row[0][1]), row[1][2]))
# Filter and print
airports = airports.filter(lambda x: x[0] in [('LGA', 'BOS'), ('BOS', 'LGA'), ('OKC', 'DFW'), ('MSP', 'ATL')])

airports.foreachRDD(printResults)
airports.foreachRDD(saveResults)

# Kafka sink
airports.foreachRDD(lambda rdd: rdd.foreachPartition(sendToKafka))

ssc.start()  # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
