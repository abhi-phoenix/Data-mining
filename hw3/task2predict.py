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
import pickle
import math

start = time.time()

def task2(sc,test_review, model):
    def cosine(x, model_dic):
        user = x[0]
        business = x[1]
        if user in model_dic['user_profile'] and business in model_dic['business_profile']:
            set_user = set(model_dic['user_profile'][user])
            set_business = set(model_dic['business_profile'][business])
            numer = len(set_user.intersection(set_business))
            denom = math.sqrt(len(set_user))*math.sqrt(len(set_business))
            if denom != 0:
                val = numer/denom
                return (user,business, val)
            else:
                return (user,business, 'notexis') #'notexis')
        else:
            return (user,business, 'notexis')
        
        
        
        
        
    textRDD = sc.textFile(test_review)
    RDD_dictionary = textRDD.map(json.loads)
    spark = SparkSession(sc)
    #print(RDD_dictionary.first())

    with open(model, 'r') as file:
        model_dic = json.load(file)
    #print(model_dic)

    user_business = RDD_dictionary.map(lambda x: (x['user_id'], x['business_id'])) #.map(lambda x: cosine(x, model_dic))
    filt = user_business.map(lambda x: cosine(x, model_dic)).filter(lambda x: x[2] != 'notexis') # 
    
    return filt.collect()
    #user_business_filt = user_business.filter(lambda x: x[2] != 'notexis').collect()
                                       
    #return user_business_filt                          
                                       
    #print(output)

    
  
if __name__ == '__main__':
    if len(sys.argv[1:]) == 3:
        test_review = sys.argv[1]
        model_file = sys.argv[2]
        output_file = sys.argv[3]
        
        conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory", "4g"))

        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")

        output = task2(sc,test_review, model_file)
        print(time.time()-start)
        with open(output_file, 'w+') as file:
            for row in output:
                temp_dict = dict()
                temp_dict['user_id'] = row[0]
                temp_dict['business_id'] = row[1]
                temp_dict['sim'] = row[2]
                
                line = json.dumps(temp_dict)+"\n"
                file.writelines(line)
            file.close()


    else:
        print("Not Enough Number of Arguments")

