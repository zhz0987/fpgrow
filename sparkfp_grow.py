from pyspark import SparkContext
sc = SparkContext()

parsedDat = [line.split() for line in open('T10I4D100K.dat').readlines()]
# print parsedDat