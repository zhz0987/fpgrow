from pyspark import SparkContext
sc = SparkContext()
import sys
import numpy as np
import fp

def createInitSet(dataSet):  
    retDict = {}  
    for trans in dataSet:  
        retDict[frozenset(trans)] = 1  
    return retDict

from pyspark.mllib.fpm import FPGrowth
from operator import add

class treeNode :  
    def __init__(self,parentNode,nameValue='',numOccur=0):  
        self.name = nameValue  
        self.count = numOccur  
        self.nodeLink = None  
        self.parent = parentNode  
        self.children = {}  
    def inc (self,numOccur):  
        self.count += numOccur  
          
    def disp(self,ind = 1):  
        print ' '*ind,self.name,'  ',self.count  
        for child in self.children.values():  
            child.disp(ind+1)

    def isRoot(self):
    	if(parent == None):
    		return True
    	else:
    		return False
    	pass

class Summary(object):
	"""docstring for summary"""
	def __init__(self):
		super(Summary, self).__init__()
		self.count = 0
		self.nodes = []

class FPtree:
	def __init__():
		self.summaries = {}
		self.root = treeNode(None)
		pass

	def add(self,transactions,count=1):
		curr = self.root
		self.rroot.count = self.rroot.count+count
		for item in transactions:
			if self.rsummaries.has_key(item[0]):
				summary = self.rsummaries[item[0]]
			else:
				summary = Summary()
				self.rsummaries[item[0]] = summary
			summary.count = count + summary.count
			child_list = curr.children
			if child_list.has_key[item[0]]:
				child = curr.child[item[0]]
			else:
				child = treeNode(curr,item)
				summary.nodes += child
			child.count = child.count + count
			curr = child

	def project(self,suffix):
		tree = FPtree()
		summary = self.summaries[suffix]
		nodes_list = summaries.nodes
		for node in nodes_list:
			curr = node.parent
			while (curr.isRoot()==False):
				tree.add((curr.nameValue),node.count)
				pass

		return tree

	def extract(self,min_count,valid):
		pass


		

def spark_fp_growth(path):
	# print "test"
	# data = sc.textFile("/home/zhz/Desktop/spark/data/mllib/sample_fpgrowth.txt")
	data = sc.textFile(path)
	transactions = data.map(lambda line: line.strip().split(' '))
	unique = transactions.map(lambda x: list(set(x))).cache()
	# print unique.top(100)
	model = FPGrowth.train(unique, minSupport=0.05, numPartitions=10)
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

import time

if __name__ == "__main__":
	a = time.time()
	# parsedDat = [line.split() for line in open('T10I4D100K.dat').readlines()]
	spark_fp_growth('kosarak.dat')
	print time.time()-a
# print createInitSet(parsedDat)
# print parsedDat
#just test 