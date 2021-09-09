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

def task3(sc,train_review,test_review,model_file, cf_type):
    def pred(x,train_dat_gr,model_bus_sim_dict,RDD_dictionary_avg_bus,RDD_dictionary_avg_user):
        user = x[0]
        business = x[1]

        user_bus_list = train_dat_gr[user]
        #print(user_bus_list)
        ## maybe store elements too
        tuples = [(i[1], i[0]) for i in user_bus_list]
#        tuples_2 = [(i[0],model_file[])]
        #print(model_bus_sim_dict[( '2YBO1LEKIgyle0uX50u15Q','9UVkAmyodpaSBm49DBKFNw')])
        tuples = [(i[1], model_bus_sim_dict[(x[1],i[0])]) for i in user_bus_list if (x[1],i[0]) in model_bus_sim_dict]
        sort_tuples = sorted(tuples, key=lambda tup:tup[1], reverse =True)
        sort_tuples_slice = sort_tuples[:3]

        num = sum([i[0]*i[1] for i in sort_tuples_slice])
        denom = sum([i[1] for i in sort_tuples_slice])
        if num != 0 and denom != 0:
            return (user,business, num/denom)
        else:
            if business in RDD_dictionary_avg_bus:
                return (user,business, RDD_dictionary_avg_bus[business])
            else:
                return (user,business, 'no')
               
        
    textRDD = sc.textFile(test_review)
    RDD_dictionary_test = textRDD.map(json.loads)
    spark = SparkSession(sc)

    trainRDD = sc.textFile(train_review)
    RDD_dictionary_train = trainRDD.map(json.loads)

    model = sc.textFile(model_file)
    RDD_dictionary_model = model.map(lambda x: json.loads(x))
    #print(RDD_dictionary_model.first())

    avg_bus = sc.textFile(avg_bus_file)
    RDD_dictionary_avg_bus = avg_bus.map(json.loads).flatMap(lambda x: x.items()).collectAsMap()
    #print(RDD_dictionary_avg_bus)

    avg_user = sc.textFile(avg_user_file)
    RDD_dictionary_avg_user = avg_user.map(json.loads).flatMap(lambda x: x.items()).collectAsMap()

    if cf_type == "item_based":
        user_bus_test = RDD_dictionary_test.map(lambda x: (x['user_id'],x['business_id']))
        #train = rdd ...
        model_bus_sim = RDD_dictionary_model.map(lambda x: ((x['b1'],x['b2']),x['sim']))
        model_bus_sim_dict = model_bus_sim.collectAsMap()

        train_dat = RDD_dictionary_train.map(lambda x: (x['user_id'], [(x['business_id'],x['stars'])]))
        train_dat_gr = train_dat.reduceByKey(lambda a,b:a+b).collectAsMap()

        user_bus_test_pred = user_bus_test.map(lambda x: pred(x,train_dat_gr,model_bus_sim_dict,RDD_dictionary_avg_bus,RDD_dictionary_avg_user))
        print(user_bus_test_pred.count(), user_bus_test.count())
        user_bus_test_pred = user_bus_test_pred.filter(lambda x: x[2]!= 'no')
        return user_bus_test_pred.collect()
        
    else:
        a = 1
        

"""        
    bus_user_star = RDD_dictionary.map(lambda x: (x['business_id'],[(x['user_id'],x['stars'])]))
                                        
    if cf_type == "item_based":
        business_list = RDD_dictionary.map(lambda x: (x['business_id'])).distinct()
        business_pairs = business_list.cartesian(business_list).filter(lambda x: x[0] != x[1])
        
        bus_user_star_group = bus_user_star.reduceByKey(lambda a,b: a+b) #.collectAsMap() #.filter(lambda x: len(x[1])>thres)
        ###
        #bus_user_star_group_userset = bus_user_star_group.map(lambda x: (x[0], (x[1], set([i[0] for i in x[1]])))).collectAsMap() #set([x[1][0] for i in x[1]])))
        #print(bus_user_star_group_userset.first())
        ###3
        bus_user_set = bus_user_star_group.map(lambda x: (x[0], set([i[0] for i in x[1]]))).collectAsMap()
        bus_user_group_star = bus_user_star_group.flatMap(lambda x: [((x[0],i[0]), i[1]) for i in x[1]]).collectAsMap()
        #print(bus_user_group_star.keys())

        business_pairs = business_pairs.map(lambda x: (x[0],x[1], bus_user_set[x[0]].intersection(bus_user_set[x[1]])))
        business_pairs_filt = business_pairs.filter(lambda x: len(x[2])>=3)
        business_pearson = business_pairs_filt.map(lambda x: pearson_coeff(x, bus_user_group_star))
        print(business_pearson.first())
        business_pearson_filt = business_pearson.filter(lambda x: x[2]>0)
        print(business_pearson_filt.collect())
        
    
    else:
        print(RDD_dictionary.first())


    #return model

"""



if __name__ == '__main__':

    if len(sys.argv[1:]) == 5:
        train_review = sys.argv[1]
        test_review = sys.argv[2]
        model_file = sys.argv[3]
        output_file = sys.argv[4]
        cf_type = sys.argv[5]

        #ALERT
        #avg_bus_file = "../resource/asnlib/publicdata/business_avg.json"
        #avg_user_file = "../resource/asnlib/publicdata/user_avg.json"
        avg_bus_file = "business_avg.json"
        avg_user_file = "user_avg.json"

        conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory", "4g"))

        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")

        output = task3(sc,train_review,test_review,model_file, cf_type)
        
        with open(output_file, 'w+') as file:

            for row in output:

                temp_dict = dict()

                temp_dict['user_id'] = row[0]

                temp_dict['business_id'] = row[1]

                temp_dict['stars'] = row[2]

                

                line = json.dumps(temp_dict)+"\n"

                file.writelines(line)

            file.close()
        


        print(time.time()-start)
    else:
        print("Not Enough Number of Arguments")
