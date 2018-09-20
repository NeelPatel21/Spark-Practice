import util as u

from pyspark import SparkConf, SparkContext
import re


def to_tuple(row):
    sp = row.split('"')
    if len(sp) >= 3:
        return sp[0]+sp[1], ''.join(sp[2:]).strip(',')
    else:
        return tuple(row.split(','))


if __name__ == '__main__':
    conf = SparkConf().setAppName("marvel").setMaster("spark://sandbox-hdp.hortonworks.com:7077").set("spark.ui.port","8081")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("./../data/mcu/edges.csv")
    rdd = rdd.map(u.parseToTuple)
    u.printf(rdd.take(25))
