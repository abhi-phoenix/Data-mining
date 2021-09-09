from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import json
import re
import collections
import time
import math
import random
from itertools import combinations
import json
import string
## import math??

start = time.time()

def task2(sc,train_review, stopwords_path):
    def tf_words(xxx,thres):
        xx= xxx[1]
        freq_dict = dict()
        max_val = -1
        for i in xx:
            if i in freq_dict:
                freq_dict[i] +=1
                if max_val <freq_dict[i]:
                    max_val = freq_dict[i]
            else:
                freq_dict[i] = 1
                if max_val <freq_dict[i]:
                    max_val = freq_dict[i]

        
        print(freq_dict.items())
        ## math.floor?
        freq_dict = dict(filter(lambda ite: ite[1] > math.floor(thres), freq_dict.items()))
        freq_dict_tf = [((xxx[0],key),val/max_val) for key,val in freq_dict.items()]
        return freq_dict_tf
        
            
    f = open(stopwords_path, "r")
    stop_words = set(f.read().splitlines())
    #print(stop_words)
    
    textRDD = sc.textFile(train_review)
    RDD_dictionary = textRDD.map(json.loads)
    spark = SparkSession(sc)

    
    #print(business.collect()[:10])

    business2= RDD_dictionary.map(lambda x: (x['business_id'], x['text'])).map(lambda x: (x[0],x[1].lower())).reduceByKey(lambda a,b: a+b) \
               .map(lambda x: (x[0], x[1].translate(str.maketrans('', '', string.digits+string.punctuation)))).map(lambda x: (x[0],x[1].split())) \
               .map(lambda x: (x[0], [i for i in x[1] if i not in stop_words]))

    total_words = business2.flatMap(lambda x: x[1]).distinct().count()
    thres = total_words*0.000001

    freq_words = business2.map(lambda x: tf_words(x,thres))
    print(freq_words.flatMap(lambda x:(x[0][1],x[0][0])).first())
    #print(freq_words.flatMap(lambda x:x).count())
    #words_idf = freq_words.flatMap(lambda x: (x[0][1],list(x[0][0]))) #.reduceByKey(lambda a,b: a+b)
    #print(words_idf.first())

    


    """   
    textRDD = sc.textFile(train_review)
    RDD_dictionary = textRDD.map(json.loads)
    spark = SparkSession(sc)
    #mapping = RDD_dictionary.map(lambda x: (x['business_id'], x['user_id'])).distinct().filter(lambda x: x[0] != None and x[1] != None).map(lambda x: (x[0], {x[1]})) #.filter(lambda x: x[2]!= None)
    business_user_group = RDD_dictionary.map(lambda x: (x['business_id'], x['user_id'])).groupByKey()
    business_user = business_user_group.mapValues(set)
    business_user_dict = business_user.collectAsMap()
    #business_user2 = RDD_dictionary.map(lambda x: (x['business_id'], {x['user_id']})).reduceByKey(lambda a,b: a+b)
    user = RDD_dictionary.map(lambda x: x['user_id']).distinct()
    user_sorted = user.sortBy(lambda user_id: user_id)
    users_all = user_sorted.collect()
    """

    


if __name__ == '__main__':
    
    if len(sys.argv[1:]) == 3:
        train_review = sys.argv[1]
        model_file = sys.argv[2]
        stopwords_path = sys.argv[3]
        

        conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory", "4g"))

        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")

        output = task2(sc,train_review, stopwords_path)

        


    else:
        print("Not Enough Number of Arguments")

