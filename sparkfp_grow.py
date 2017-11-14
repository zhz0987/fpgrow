from pyspark import SparkContext
sc = SparkContext()
import sys

def createInitSet(dataSet):  
    retDict = {}  
    for trans in dataSet:  
        retDict[frozenset(trans)] = 1  
    return retDict

from pyspark.mllib.fpm import FPGrowth

def spark_fp_growth(path):
	# print "test"
	# data = sc.textFile("/home/zhz/Desktop/spark/data/mllib/sample_fpgrowth.txt")
	data = sc.textFile(path)
	transactions = data.map(lambda line: line.strip().split(' '))
	unique = transactions.map(lambda x: list(set(x))).cache()
	print unique.top(100)
	model = FPGrowth.train(unique, minSupport=0.1, numPartitions=10)
	result = model.freqItemsets().collect()
	for fi in result:
	    print(fi)

def seq_fp_growth(path):
	data = sc.textFile(path)
	transactions = data.map(lambda line: line.strip().split(' '))
	unique = transactions.map(lambda x: list(set(x))).cache()
	print unique.top(100)

def pal_fp_growth(path):
	data = sc.textFile(path)
	transactions = data.map(lambda line: line.strip().split(' '))
	unique = transactions.map(lambda x: list(set(x))).cache()
	print unique.top(100)

if __name__ == "__main__":
	parsedDat = [line.split() for line in open('T10I4D100K.dat').readlines()]
	spark_fp_growth('kosarak.dat')

# print createInitSet(parsedDat)
# print parsedDat
#just test 