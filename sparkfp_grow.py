from pyspark import SparkContext
sc = SparkContext()
import sys
import numpy as np

def createInitSet(dataSet):  
    retDict = {}  
    for trans in dataSet:  
        retDict[frozenset(trans)] = 1  
    return retDict

from pyspark.mllib.fpm import FPGrowth
from operator import add

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

def seq_genFreqItems(data, min_count):
	print data.top(100)
	# print data.flatMap(lambda a : a).top(10)
	print min_count
	freqItems = data.flatMap(lambda a : a)\
			.map(lambda a:(a,1)) \
			.reduceByKey(add) \
			.filter(lambda (x,y): y>min_count) \
			.sortBy(lambda (x,y): y)
	print freqItems.top(10)

	return freqItems

	pass

def seq_genFreqItemsets(data,min_count,freqItems):
	# need FP structure
	pass

def seq_fp_growth(path,minSupport=0.1,numPartitions=10):
	# data = sc.textFile(path)
	data = sc.textFile("/home/zhz/Desktop/spark/data/mllib/sample_fpgrowth.txt")
	transactions = data.map(lambda line: line.strip().split(' '))
	unique = transactions.map(lambda x: list(set(x))).cache()
	print unique.top(100)
	count = unique.count()
	if(minSupport<1):
		min_count = np.ceil(count*minSupport)
	else:
		min_count = minSupport
	# num_parts = 
	# partition
	freqItems = seq_genFreqItems(unique, min_count)
	print freqItems.top(2)
	freq_Item =  seq_genFreqItemsets(unique, min_count, freqItems)



def pal_fp_growth(path):
	data = sc.textFile(path)
	transactions = data.map(lambda line: line.strip().split(' '))
	unique = transactions.map(lambda x: list(set(x))).cache()
	print unique.top(100)

if __name__ == "__main__":
	parsedDat = [line.split() for line in open('T10I4D100K.dat').readlines()]
	seq_fp_growth('kosarak.dat')

# print createInitSet(parsedDat)
# print parsedDat
#just test 