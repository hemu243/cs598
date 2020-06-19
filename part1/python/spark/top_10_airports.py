# import pyspark
# from pyspark import SparkContext
# sc =SparkContext()

from __future__ import print_function
from pyspark import SparkContext
import sys
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: testjob  ", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Top10popularairports")
    dataTextAll = sc.textFile(sys.argv[1])
    dataRDD = dataTextAll.map(lambda line: line.split()).filter(lambda tuple: len(tuple) == 2).filter(lambda tuple: len(tuple[0]) == 3).map(lambda tuple: (tuple[1], tuple[0])).sortByKey(ascending=False).take(10)
    dataRDD.saveAsTextFile(sys.argv[2])
    sc.stop()
