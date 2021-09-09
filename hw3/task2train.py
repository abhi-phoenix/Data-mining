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
import pickle
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


        #print(freq_dict.items())
        ## math.floor?
        freq_dict = dict(filter(lambda ite: ite[1] > math.floor(thres), freq_dict.items()))
        freq_dict_tf = [((xxx[0],key),val/max_val) for key,val in freq_dict.items()]
        return freq_dict_tf

    def word_derive(x,bus_dict):
        user = x[0]
        busi = x[1]
        words = set()
        for business in busi:
            words = words.union(bus_dict[business])
        return (user,words)

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

    business_total= RDD_dictionary.map(lambda x: (x['business_id'],1)).reduceByKey(lambda a,b: a+b).count()



    total_words = business2.flatMap(lambda x: x[1]).distinct().count()
    thres = total_words*0.000001

    freq_words = business2.map(lambda x: tf_words(x,thres))
    freq_words_flat = freq_words.flatMap(lambda x: x)
    freq_tuples = freq_words_flat.map(lambda x: (x[0][1],[x[0][0]]))
    freq_tuples_idf = freq_tuples.reduceByKey(lambda a,b: a+b).map(lambda x: (x[0],math.log(business_total/len(set(x[1])),2))).collectAsMap()
    #tf_idf = freq_words_flat.map(lambda x: )
    #print(freq_tuples_idf)
    freq_words_tf_idf = freq_words.map(lambda xx: [((x[0][0],x[0][1]),x[1]*freq_tuples_idf[x[0][1]]) for x in xx])
    freq_tf_idf_take_top = freq_words_tf_idf.map(lambda x: sorted(x, key = lambda vl: vl[1], reverse=True)[:200])
    freq_tf_idf_take_top_words = freq_tf_idf_take_top.map(lambda x: [i[0] for i in x])

    #####
    flat_all_words = freq_tf_idf_take_top_words.flatMap(lambda x: [(i[1],1) for i in x]).reduceByKey(lambda a,b: a+b).keys()
    indexes = range(flat_all_words.count())
    words_indexes = dict(zip(flat_all_words.collect(), indexes))
    #####


    business_prof = freq_tf_idf_take_top_words.map(lambda x: (x[0][0], {words_indexes[i[1]] for i in x})).map(lambda x: (x[0], list(x[1]))).collectAsMap()
    #business_prof = freq_tf_idf_take_top_words.map(lambda x: (x[0][0], {i[1] for i in x})).collectAsMap()
    #print(business_prof.first())

    #business_prof12 = freq_tf_idf_take_top_words.map(lambda x: (x[0][0], {words_indexes[i[1]] for i in x}))
    #business_prof13 = freq_tf_idf_take_top_words.map(lambda x: (x[0][0], {i[1] for i in x}))
    #print(business_prof12.first())
    #print(business_prof13.first())
    #print(len(business_prof12.first()[1]))
    #print(len(business_prof13.first()[1]))


    user = RDD_dictionary.map(lambda x: (x['user_id'],[x['business_id']])).reduceByKey(lambda a,b:a+b).map(lambda x: (x[0],set(x[1]))).map(lambda x: word_derive(x,business_prof)).map(lambda x: (x[0],list(x[1]))).collectAsMap()
    #print(user.first())

    #print(freq_tf_idf_take_top.first())
    #print(freq_tf_idf_take_top_words.first())
    #print(words_indexes)
    #print(flat_all_words.count())

    model = dict()
    model['user_profile'] =user
    model['business_profile'] = business_prof

    return model



    #print(freq_words_tf_idf_take_top.first())



    #print(freq_words.flatMap(lambda x:x).first())
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
        print(type(output),type(output['user_profile']))


        with open(model_file, 'w') as file:
            json.dump(output, file)
        #with open(model_file, 'wb') as file:
        #    pickle.dump(output, file)


        print(time.time()-start)
    else:
        print("Not Enough Number of Arguments")
