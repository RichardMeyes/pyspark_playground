import numpy as np
from pyspark import SparkContext, SparkConf


if __name__ == "__main__":

    conf = SparkConf().setAppName("test_app").setMaster("local")
    sc = SparkContext(conf=conf)

    """
    # equivalent to
    sc = SparkContext("local", appName="test_app")
    """

    # read California housing data set into resilient distributed data set
    rdd_header = sc.textFile('../data/cal_housing/header.txt')
    rdd_data = sc.textFile('../data/cal_housing/data.txt')

    # read header
    header = rdd_header.take(24)
    for line in header:
        print(line)

    # read data
    data = rdd_data.take(5)
    for line in data:
        print(line)