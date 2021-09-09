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
import binascii
import json

start = time.time()

def generate_prime(m,number_of_primes):
    def prime(count):
        end = int(math.sqrt(count))+1
        for xx in range(2,end):
            if count%xx == 0:
                return False
        return True


    primes  = []
    count = m+1
    while len(primes) != number_of_primes:
        if prime(count) == True:
            primes.append(count)
            count+=1
        else:
            count+=1
    return primes

def task1(sc,first_json, second_json, output_file):
    
    def hash_func_tuples(length_users_all, hash_functions_count):
        list_ = list(range(1,length_users_all))
        alpha = random.sample(list_,hash_functions_count)
        beta = random.sample(list_,hash_functions_count)
        peta = generate_prime(length_users_all, hash_functions_count)
        ## HERE set
        tuples = set(zip(alpha,beta,peta))
        if len(tuples) != len(alpha):
            while True:
                a = random.sample(list_,hash_functions_count)
                b = random.sample(list_,hash_functions_count)
                p = generate_prime(length_users_all, hash_functions_count)
                new_tuples = list(zip(alpha,beta,peta))
                for i in new_tuples:
                    tuples.add(i)
                    if len(tuples) == len(alpha):
                        return list(tuples)
        else:
            return list(tuples)
    """
    def get_signature(slice_row, tuples, length_users_all):
        output = [99999999 for i in range(len(tuples))]
        for user in slice_row:
            for tup in range(len(tuples)):
                hashfunction = ((tuples[tup][0]*user+tuples[tup][1])%tuples[tup][2])%length_users_all
                if hashfunction<output[tup]:
                    output[tup] = hashfunction
        return output
    """
    def generate_hashes(x, hash_func_tuples,m):
        funcs_all = []
        
        for i in hash_func_tuples:
            print(i[0], i[1], i[2])
            val  = ((i[0]*x+i[1])%i[2])%m
            print(type(val))
            funcs_all.append(val)
        return funcs_all
    
    hash_functions_count = 80
    m = 1200
    filter_arr = [0 for _ in range(m)]
    textRDD = sc.textFile(first_json)
    RDD_dictionary = textRDD.map(json.loads)
    spark = SparkSession(sc)
    textRDD2 = sc.textFile(second_json)
    RDD_dictionary2 = textRDD2.map(json.loads)
    #print(RDD_dictionary.collect())
    mapping = RDD_dictionary.map(lambda x: x['name'])
    mapping2 = RDD_dictionary2.map(lambda x: x['name'])
    hashed_map = mapping.map(lambda x: int(binascii.hexlify(x.encode('utf8')),16))
    hashed_map2 = mapping2.map(lambda x: int(binascii.hexlify(x.encode('utf8')),16))
    
    hash_func_tuples = hash_func_tuples(m, hash_functions_count)

    hash_funcs_map = mapping.map(lambda x: generate_hashes(x, hash_func_tuples,m))
    for arr in hash_funcs_map.collect():
        for hash_val_arr in arr:
            filter_arr[hash_val_arr] = 1
    
    hash_funcs_map2 = mapping2.map(lambda x: generate_hashes(x, hash_func_tuples,m))
    with open(output_file, 'w+') as file:
        for arr in hash_funcs_map2.collect():
            ###
            flag = 1
            for hash_val_arr in arr:
                if filter_arr[hash_val_arr] != 1:
                    flag = 0
            if flag ==1:
                file.write('T'+'\t')
            else:
                file.write('F'+'\t')
            
        file.close()
    
    #print(int(binascii.hexlify(mapping.first().encode('utf8')),16))

    
    

    """
    #mapping = RDD_dictionary.map(lambda x: (x['business_id'], x['user_id'])).distinct().filter(lambda x: x[0] != None and x[1] != None).map(lambda x: (x[0], {x[1]})) #.filter(lambda x: x[2]!= None)
    business_user_group = RDD_dictionary.map(lambda x: (x['business_id'], x['user_id'])).groupByKey()
    business_user = business_user_group.mapValues(set)
    business_user_dict = business_user.collectAsMap()
    #business_user2 = RDD_dictionary.map(lambda x: (x['business_id'], {x['user_id']})).reduceByKey(lambda a,b: a+b)
    user = RDD_dictionary.map(lambda x: x['user_id']).distinct()
    user_sorted = user.sortBy(lambda user_id: user_id)
    users_all = user_sorted.collect()

##  2
    dic_user = dict()
    length_users_all = len(users_all)
    indexes_users = list(range(length_users_all))
    for user_index in range(len(users_all)):
        dic_user[users_all[user_index]] = indexes_users[user_index]    

    business = RDD_dictionary.map(lambda x: x['business_id']).distinct()
    business_sorted = user.sortBy(lambda business_id: business_id)
    business_all = business_sorted.collect()

# 3
    dic_business = dict()
    length_business_all = len(business_all)
    indexes_business = list(range(length_business_all))
    for business_index in range(len(business_all)):
        dic_business[business_index] = business_index


    business_user_transform_char = business_user.map(lambda x: (x[0],[dic_user[i] for i in x[1]]))

    tuples = hash_func_tuples(length_users_all, hash_functions_count)
    signature_matr = business_user_transform_char.map(lambda x: (x[0],get_signature(x[1],tuples, length_users_all)))
    print(signature_matr.collect())

    Lsh_bands= signature_matr.flatMap(lambda x: LSH(x,bands, rows))
    #print(Lsh_bands.collect())
    reduce_lsh = Lsh_bands.groupByKey()
    #print(reduce_lsh)
    second_map_cleared = reduce_lsh.mapValues(list)
    second_map_filtered = second_map_cleared.filter(lambda x:len(x[1])>1) #.= ? hash bands what, key? (band, tuple)
    cands = second_map_filtered.flatMap(lambda x: list(combinations(x[1],2))).distinct()
    #print(cands.collect())
    jaccard_similarity = cands.map(lambda x: jaccard(x, business_user_dict)).filter(lambda x: x[2]>=0.055).collect() #.sort()
    #print(jaccard_similarity)
    print(jaccard_similarity)
    #print(business_user.collect())
    
    return jaccard_similarity
    """
    
    return 1
    


if __name__ == '__main__':
    if len(sys.argv[1:]) == 3:
        first_json = sys.argv[1]
        second_json = sys.argv[2]
        output_file = sys.argv[3]

        conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))

        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")

        output = task1(sc,first_json, second_json, output_file)
        """
        with open(output_file, 'w+') as file:
            for row in output:
                temp_dict = dict()
                temp_dict['b1'] = row[0]
                temp_dict['b2'] = row[1]
                temp_dict['sim'] = row[2]
                
                line = json.dumps(temp_dict)+"\n"
                file.writelines(line)
            file.close()
        """
        
        #print(time.time()-start)

    else:
        print("Not Enough Number of Arguments")
