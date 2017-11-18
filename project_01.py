from pyspark import SparkContext
sc = SparkContext()
from pyspark.mllib.fpm import FPGrowth

class treeNode :  
    def __init__(self,nameValue,numOccur,parentNode):  
        self.name = nameValue  
        self.count = numOccur  
        self.nodeLink = None  
        self.parent = parentNode  
        self.children = {}  
    def inc (self,numOccur):  
        self.count += numOccur  
          
    def disp(self,ind = 1):  
        # print ' '*ind,self.name,'  ',self.count  
        for child in self.children.values():  
            child.disp(ind+1)  
  
def createTree(dataSet,minSup=1):  
    headerTable = {}  
    for trans in dataSet:  
        for item in trans:  
            headerTable[item] = headerTable.get(item,0)+ dataSet[trans]  
    for k in headerTable.keys():  
        if headerTable[k] < minSup:  
            del(headerTable[k])
    freqItemSet = set(headerTable.keys())  
    if len(freqItemSet) == 0 : return None,None  
    for k in headerTable:  
        headerTable[k] = [headerTable[k],None]  
    retTree = treeNode('Null Set',1,None)  
    for tranSet ,count in dataSet.items():  
        localD = {}  
        for item in tranSet:  
            if item in freqItemSet:  
                localD[item] = headerTable[item][0]  
        if len(localD) > 0:  
            orderedItems = [v[0] for v in sorted(localD.items(),key = lambda p:p[1],reverse = True)]  
            updateTree(orderedItems,retTree,headerTable,count)  
    return retTree,headerTable  
  
def updateTree(items,inTree,headerTable,count):  
    if items[0] in inTree.children:  
        inTree.children[items[0]].inc(count)  
    else:  
        inTree.children[items[0]] = treeNode(items[0],count,inTree)  
        if headerTable[items[0]][1] ==None:  
            headerTable[items[0]][1] = inTree.children[items[0]]  
        else:  
            updateHeader(headerTable[items[0]][1], inTree.children[items[0]])  
    if len(items) > 1:  
        updateTree(items[1::], inTree.children[items[0]], headerTable, count)  
          
def updateHeader(nodeToTest,targetNode):  
    while (nodeToTest.nodeLink !=  None):  
        nodeToTest = nodeToTest.nodeLink  
    nodeToTest.nodeLink = targetNode  
  
def createInitSet(dataSet):  
    retDict = {}  
    for trans in dataSet:  
        retDict[frozenset(trans)] = retDict.get(frozenset(trans), 0) + 1  #headerTable.get(item,0)+ dataSet[trans]
    return retDict  
  
def ascendTree(leafNode,prefixPath):  
    if leafNode.parent !=None:  
        prefixPath.append(leafNode.name)  
        ascendTree(leafNode.parent, prefixPath)  
  
def findPrefixPath(basePat,treeNode):  
    condPats={}  
    while treeNode != None:  
        prefixPath = []  
        ascendTree(treeNode, prefixPath)  
        if len(prefixPath) > 1:  
            condPats[frozenset(prefixPath[1:])] = treeNode.count  
        treeNode = treeNode.nodeLink  
    return condPats  
  
def mineTree(inTree,headerTable,minSup,preFix,freqItemList):
    if headerTable == None:
        return
    bigL = [v[0] for v in sorted(headerTable.items(),key=lambda p:p[1])]  
    for basePat in bigL:  
        newFreqSet = preFix.copy()  
        newFreqSet.add(basePat)
        freqItemList.append(newFreqSet)
        condPattBases = findPrefixPath(basePat, headerTable[basePat][1])  
        myCondTree,myHead = createTree(condPattBases, minSup)   
        if myHead!= None: 
            mineTree(myCondTree, myHead, minSup, newFreqSet, freqItemList)  

def createGlobalTree(headerTable, dataSet,minSup=1):  
    freqItemSet = set(headerTable.keys())  
    if len(freqItemSet) == 0 : return None,None  
    for k in headerTable:  
        headerTable[k] = [headerTable[k],None]  
    retTree = treeNode('Null Set',1,None)  
    for tranSet ,count in dataSet.items():  
        localD = {}  
        for item in tranSet:  
            localD[item] = headerTable[item][0]  
        if len(localD) > 0:  
            orderedItems = [v[0] for v in sorted(localD.items(),key = lambda p:p[1],reverse = True)]  
            updateTree(orderedItems,retTree,headerTable,count)  
    return retTree,headerTable  
def findGlobalPrefixPath(basePat,treeNode):  
    condPats={}  
    while treeNode != None:  
        prefixPath = []  
        ascendTree(treeNode, prefixPath)  
        if len(prefixPath) > 0:  
            condPats[frozenset(prefixPath[1:])] = treeNode.count  
        treeNode = treeNode.nodeLink  
    return condPats 
    
def Distribute_tran(inTree,headerTable,minSup,preFix):
    trans_dict = {}
    bigL = [v[0] for v in sorted(headerTable.items(),key=lambda p:p[1])]  
    for basePat in bigL:  
        #newFreqSet = preFix.copy()
        #print 'before', basePat, newFreqSet  
        #newFreqSet.add(basePat)
        #print newFreqSet
        #freqItemList.append(newFreqSet)

        #print 'basePat',basePat
        condPattBases = findGlobalPrefixPath(basePat, headerTable[basePat][1])
        trans_dict[basePat] = condPattBases
    return trans_dict

import time 

if __name__ == '__main__':
    a = time.time()
    print a
    data = [line.split() for line in open('kosarak.dat').readlines()]

    numPartitions = 15
    transactions = sc.parallelize(data, numPartitions).cache()

    min_s = 50000

    def concucate_list(it):
        for item in it:
            for r in item:
                yield (r,1)

    rdd = transactions.flatMap(lambda a : a)\
                        .map(lambda a:(a,1)) \
                        .reduceByKey(lambda x, y: x+y) \
                        .filter(lambda x: x[1] > min_s)

    head_list = rdd.collect()
    # print(head_list)
    freq_list = {}
    for item in head_list:
        freq_list[item[0]] = item[1]
    # print freq_list
    broad_freq_list =  sc.broadcast(freq_list)

    print time.time()
    def filter_item(it):
        for items in it:
            new_tran = []
            for item in items:
                if item in broad_freq_list.value.keys():
                    new_tran.append(item)
            if len(new_tran) > 0:
                yield new_tran


    transactions = transactions.mapPartitions(filter_item)#.filter(lambda x: len(x) > 0)
    processed_tran = transactions.collect()
    print time.time()
    # print(len(processed_tran))

    G_Set = createInitSet(processed_tran)
    # print G_Set
    G_FPtree ,G_HeaderTab = createGlobalTree(freq_list, G_Set, min_s)
    G_Items = Distribute_tran(G_FPtree, G_HeaderTab, min_s, set([]))
    # print G_Items
    new_tran = []
    for key, value in G_Items.items():
        new_tran.append((key,value))
    #print new_tran
    # print(100)
    new_trans = sc.parallelize(new_tran, numPartitions)
    print time.time()
    # new_trans.take(5)

    distribute_dict = {}
    for i in range(len(head_list)):
        distribute_dict[head_list[i][0]] = i/numPartitions
    # print distribute_dict

    board_cast_distribute_dict = sc.broadcast(distribute_dict)

    def partition_fun(x):
        return board_cast_distribute_dict.value[x]

    new_trans = new_trans.partitionBy(numPartitions, partition_fun)
    print time.time()
    # new_trans.collect()

    def partitionsize(it): 
        s = 0
        for i in it:
            s += 1
        yield s

    def Golbal_train(it):
        for item in it:
            initSet = item[1]
            myFPtree , myHeaderTab = createTree(initSet, min_s)
            freqItems = [set([item[0]])]
            mineTree(myFPtree, myHeaderTab, min_s, set([item[0]]), freqItems)
            yield freqItems

    result = new_trans.mapPartitions(Golbal_train).collect()

    print time.time()

    final_result = []
    for item in result:
        final_result += item
    print final_result


    print time.time()-a

