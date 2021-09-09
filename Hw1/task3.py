from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import json
import re
import collections
import time

start = time.time()

def task3(sc,path, partition_type, n_partitions,n):
    output = dict()
    textRDD = sc.textFile(path)

    RDD_dictionary = textRDD.map(json.loads)
    ##
    spark = SparkSession(sc)
    #print(spark.first())
    ##
    if partition_type == "default":

        mapping = RDD_dictionary.map(lambda x: (x['business_id'], x['text'])).filter(lambda x: x[1] != None).map(lambda x: (x[0],1))
        reduce = mapping.reduceByKey(lambda a, b: a+b).filter(lambda x: x[1] >n)   # None)      ).map(lambda x: (x[1],x[0])).sortByKey(ascending=False).
        # BELERT
        time.sleep(5)

        #print('time',time.time()-start)
        output["n_partitions"] = mapping.getNumPartitions()
        #rss = time.time()
        #rprint([len(i) for i in mapping.glom().collect()])
        #rprint('pin 1',time.time()-ss)
        #print('impip')
        #sst = time.time()
        #print(mapping.glom().collect())
        #print('pin 2',time.time()-sst)
        sst = time.time()
        output["n_items"]=mapping.glom().map(len).collect()  #####
        output["result"]=[[i[0],i[1]] for i in reduce.collect()]

        #print('pin 2',time.time()-sst)
        #print(sc.parallelize([('ac',1), ('qd',1),('ac',1) , ('qd',1)]).distinct().count())
        #print(reduce.map(lambda x: [x[0],x[1]]).collect())
        #print(mapping.getNumPartitions())
        #print(reduce.getNumPartitions())

    #mapping = RDD_dictionary.map(lambda business: (business['business_id'],business['categories']))
        #print(spark.createDataFrame(RDD_dictionary))
        #print(spark.createDataFrame(RDD_dictionary.map(lambda x: x)))
    #print(mapping.getNumPartitions())
    # get partitionitems
    # default
    # get numpartiotions
    ## partitionBy(groupID,number of partitions)
    else:
        def sum_ID(iterator):
            summ =0
            temp =""
            for bus_tup in iterator:
                temp = bus_tup[0]
                summ+= bus_tup[1]
            yield (temp,summ)
        def PartitionLen(iterator):
            return [len(list(iterator))]
            #yield (iterator[0][0],sum(bus_tup[1] for bus_tup in iterator))

        mapping = RDD_dictionary.map(lambda x: (x['business_id'], x['text'])).filter(lambda x: x[1] != None).map(lambda x: (x[0],1))
        #num_partitions = mapping.distinct().count()
        ## alerts
        partitioning = mapping.partitionBy(n_partitions, hash)
        #print('here',num_partitions)
        #partitioning = mapping.partitionBy(10)
        output["n_partitions"] = n_partitions

        #output["n_items"]=mapping.glom().map(len).collect()
        output["n_items"]= partitioning.mapPartitions(PartitionLen).collect()

        #reducer2 = partitioning.mapPartitions(sum_ID).filter(lambda x: x[1] >n)
        ####reducer2 = partitioning.mapPartitions(lambda x: [sum(1 for _ in x)] ).collect()   #.filter(lambda x: x[1] >n)
        ####print(reducer2)
        reduce = partitioning.reduceByKey(lambda a, b: a+b).filter(lambda x: x[1] >n)   # None)      ).map(lambda x: (x[1],x[0])).sortByKey(ascending=False).
        output["result"]=[[i[0],i[1]] for i in reduce.collect()]

        #print('time',time.time()-start)

        #print([len(i) for i in mapping.glom().collect()])
        #print(reduce.collect())
        ####print(mapping.glom().collect())


    return output



if __name__ == '__main__':
    if len(sys.argv[1:]) == 5:
        review_file_path = sys.argv[1]
        output_file_path = sys.argv[2]
        partition_type = sys.argv[3]
        n_partitions = int(sys.argv[4])
        n = int(sys.argv[5])


        conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))

        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")
        #print('lapioxs',type(n_partitions), type(n))



        # ALERT
        output = task3(sc,review_file_path, partition_type, n_partitions,n )

        with open(output_file_path, 'w') as outfile:
            json.dump(output, outfile)
        #task2(sc,business_file_path)
        #print(time.time()-start)

    else:
        print("Not Enough Number of Arguments")

"""

    data2 = spark.read.json(review_file_path)
    print(data2.columns)
    reviews = data2[['review_id']]
    reviews_date = data2[['review_id','date']]
    mapper = reviews.rdd.map(lambda review: (review,1))
    reducer = mapper.reduceByKey(lambda a, b: a+b)
    #print('Oswald',len(mapper.reduceByKey(lambda a, b: a+b).collect()))
    mapper_date = reviews_date.rdd.map(lambda review: (review['review_id'],review['date'].split('-')[0]))
    #print('Doraemon',mapper_date.reduceByKey(lambda a, b: a+b).collect())
    #print('timmon&pumba',data2[['business_id','review_id']].rdd.map(lambda review: (review['review_id'],review['date'].split('-')[0])).reduceByKey(lambda a, b: a+b).collect()   )
    print(data2)

    print(data2.first())

###
    path = './business.json'
    data = spark.read.json(path)
    data_req = data[['categories']]
    mapper = data_req.rdd.flatMap(lambda line: line.split(','))

 """

"""
    mapper = data.flatMap(lambda line: line['categories'].split(',')).map(lambda word: (word,1)).collect()
    #mapper = data.map(lambda word: (word,1)).collect()

    mapper_counts = data.rdd.flatMap(lambda line: line['categories'].split(',')).map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b).collect()

    #print(data['categories'])
    for i in mapper_counts:
        print('count',i)
    category = data.map()

#data = sc.textFile('/usr/local/spark/README.md')

#path = "examples/src/main/resources/people.json"
#peopleDF = spark.read.json(path)
"""
#r#print('heree',data_req.count())
#r#print('heree2',data_req.first())







    #mapper2 = RDD_dictionary.map(lambda review: (review['date'].split('-')[0]==y,1))
    #reducer2 = mapper2.reduceByKey(lambda a, b: a+b)
    #print('Timon&Pumba, number of reviews in a given year',reducer2.collect())

    ## can use count
    #print('1',RDD_dictionary.map(lambda review: review['review_id']).filter(lambda review: review==None).collect())

##    print('Timon&Pumba, number of reviews in a given year',mapper2.filter(lambda review: review==y).map(lambda review: (review,1)).reduceByKey(lambda a, b: a+b).collect())



#IMP#    reducer3 = mapper3.reduceByKey(lambda a, b: a+b)

   #print(RDD_dictionary.map(lambda review: (review['date'].split('-')[0],1)).reduceByKey(lambda a, b: a+b).collect())
#    print('Timon&Pumba',len(reducer3.collect()))
#    print('business distinct: ', mapper3.keys().distinct().count())




#    mapper = RDD_dictionary.map(lambda review: review['review_id']).filter(lambda review: review!=None).map(lambda review: (review,1))
#    reducer = mapper.reduceByKey(lambda a, b: a+b)
#    print('Oswald, total number of reviews',reducer.count())
#    output['A':reducer.count()]

#    mapper2 = RDD_dictionary.map(lambda review: review['date'].split('-')[0])
#    reducer2 = mapper2.filter(lambda review: review==y).map(lambda review: (review,1)).reduceByKey(lambda a, b: a+b)
#    print(reducer2.collect()[0][1])
#    output['B':reducer2]


#    mapper3 = RDD_dictionary.filter(lambda review: review['review_id']!=None and review['text']!=None ).map(lambda review: (review['business_id'],1)) # if review['review_id'] != None )
#    output['C':mapper3.keys().distinct().count()]

#    mapper4 = RDD_dictionary.filter(lambda review: review['review_id']!=None and review['text']!=None ).map(lambda review: (review['user_id'],1)) # if review['review_id'] != None )
#    reducer4 = mapper4.reduceByKey(lambda a, b: a+b).map(lambda var: (var[1],var[0])).sortByKey(ascending=False)
#    top_em = [i[1] for i in reducer4.top(m)]
#    print('Timon&Pumba',top_em)
#    output['D':top_em]


#    punc = ['(', '[', ',', '.', '!', '?', ':', ';', ']', ')']
#    rx = '[' + re.escape(''.join(punc)) + ']'
#    f = open("./stopwords", "r")
#    stop_words = set(f.read().splitlines())

#    maper6 = RDD_dictionary.flatMap(lambda line: re.sub(rx, '', re.sub(rx, '', line['text'])).split()).map(lambda word: word.lower())
#    mapper6 = maper6.filter(lambda word: word not in stop_words).map(lambda x: (x,1))
#    words_freq = mapper6.reduceByKey(lambda a, b: a+b).map(lambda var: (var[1],var[0])).sortByKey(ascending=False)
    #print(words_freq.top(n))
#    top_em2 = [i[1] for i in words_freq.top(n)]
#    print('debug', top_em2)
#    output['D':top_em2]

    #mapper6 = mapper6.filter(lambda x: x[0] not in stop_words)
    #reducer6 = mapper6.reduceByKey(lambda a, b: a+b).collect()
    #print(reducer6.sortByKey(ascending=False).top(n))
