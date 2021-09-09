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
from itertools import combinations
## import math??

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


def task3(sc,train_review, cf_type):
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

    def get_signature(slice_row, tuples, length_users_all):

        output = [99999999 for i in range(len(tuples))]

        for user in slice_row:

            for tup in range(len(tuples)):

                hashfunction = ((tuples[tup][0]*user+tuples[tup][1])%tuples[tup][2])%length_users_all

                if hashfunction<output[tup]:

                    output[tup] = hashfunction

        return output



    def LSH(x,bands, rows):

        output = []

        counter = 0

        rows = int(rows)

        for i in range(bands):

            band_rows = []

            for j in range(rows):

                band_rows.append(x[1][counter])

                counter+=1



            output.append(((i,tuple(band_rows)),x[0]))

        return output



    def jaccard(x, business_user_dict):

        set1 = business_user_dict[x[0]]

        set2 = business_user_dict[x[1]]

        numer = len(set1.intersection(set2))

        denom = len(set1.union(set2))

        listt = [x[0],x[1]]

        one, two = sorted(listt)

        ## sort HERE

        return (one,two,numer/denom)
    def struct_dre(arr):
        aa = dict()
        for i in arr:
            aa[i[0]]=i[1]
        return aa
    def pearson_coeff(x, bus_user_dict):
        #print(x)
        business_0 = x[0]
        business_1 = x[1]
        users_common = x[2]
        business_0_list = [bus_user_dict[x[0]][i] for i in users_common]
        business_1_list = [bus_user_dict[x[1]][i] for i in users_common]
        averg0 = sum(business_0_list)/len(business_0_list)
        averg1 = sum(business_1_list)/len(business_1_list)

        numerat = sum([(business_0_list[i]-averg0)*(business_1_list[i]-averg1) for i in range(len(business_0_list))])
        denom = math.sqrt(sum([(j-averg0)**2 for j in business_0_list]))*math.sqrt(sum([(k-averg1)**2 for k in business_1_list]))
        if denom != 0 and numerat != 0:
            coeff = numerat/denom
            return (business_0,business_1,coeff)
        else:
            return (business_0,business_1,-10 )

    thres = 3

    textRDD = sc.textFile(train_review)
    RDD_dictionary = textRDD.map(json.loads)
    spark = SparkSession(sc)
    bus_user_star = RDD_dictionary.map(lambda x: (x['business_id'],[(x['user_id'],x['stars'])]))

    if cf_type == "item_based":
        business_list = RDD_dictionary.map(lambda x: (x['business_id'])).distinct().collect()
        business_pairs = list(combinations(business_list,2))

        bus_user_star_group = bus_user_star.reduceByKey(lambda a,b: a+b) #.collectAsMap() #.filter(lambda x: len(x[1])>thres)
        ###
        #bus_user_star_group_userset = bus_user_star_group.map(lambda x: (x[0], (x[1], set([i[0] for i in x[1]])))).collectAsMap() #set([x[1][0] for i in x[1]])))
        #print(bus_user_star_group_userset.first())
        ###3
        bus_user_set = bus_user_star_group.map(lambda x: (x[0], set([i[0] for i in x[1]]))).collectAsMap()
        bus_user_group_star = bus_user_star_group.map(lambda x: (x[0],struct_dre(x[1]))).collectAsMap()
        #print(bus_user_group_star.first())
        #bus_user_group_star = bus_user_star_group.flatMap(lambda x: [((x[0],i[0]), i[1]) for i in x[1]]).collectAsMap()
        #print(bus_user_group_star.keys())
        fin = []
        for elem in range(len(business_pairs)):
            temp = sorted([business_pairs[elem][0],business_pairs[elem][1]])
            user_co = bus_user_set[temp[0]].intersection(bus_user_set[temp[1]])
            temp.append(user_co)
            if len( user_co)>=3:
                fin.append(pearson_coeff(temp, bus_user_group_star))





        #business_pairs = business_pairs.map(lambda x: (x[0],x[1], bus_user_set[x[0]].intersection(bus_user_set[x[1]])))

        #business_pairs_filt = business_pairs.filter(lambda x: len(x[2])>=3)
        #print(business_pairs_filt.count())
        #business_pearson = business_pairs_filt.map(lambda x: pearson_coeff(x, bus_user_group_star))
        #print(business_pearson.first())

        #business_pearson_filt = business_pearson.filter(lambda x: x[2]>0)
        #print(business_pearson_filt.first(),business_pearson_filt.count())
        #ls_c = business_pearson_filt.collect()
        return fin


    else:
        def small(x):
            temp = tuple(sorted([x[0],x[1]]))
            return temp
        hash_functions_count = 40
        bands = 40
        rows = hash_functions_count/ bands
        user_bus_star = RDD_dictionary.map(lambda x: (x['user_id'],[(x['business_id'],x['stars'])]))
        user_bus_star_group = user_bus_star.reduceByKey(lambda a,b: a+b)
        user_bus_sett = user_bus_star_group.map(lambda x: (x[0], set([i[0] for i in x[1]])))
        user_bus_set = user_bus_sett.collectAsMap()
        user_bus_group_star = user_bus_star_group.map(lambda x: (x[0],struct_dre(x[1]))).collectAsMap()

        business = RDD_dictionary.map(lambda x: x['business_id']).distinct()
        business_sorted = business.sortBy(lambda business_id: business_id)
        business_all = business_sorted.collect()

        dic_business = dict()
        length_business_all = len(business_all)
        indexes_business = list(range(length_business_all))
        for business_index in range(len(business_all)):
            dic_business[business_all[business_index]] = business_index

        user_business_transform_char = user_bus_sett.map(lambda x: (x[0],[dic_business[i] for i in x[1]]))
        tuples = hash_func_tuples(length_business_all, hash_functions_count)
        signature_matr = user_business_transform_char.map(lambda x: (x[0],get_signature(x[1],tuples, length_business_all)))
        Lsh_bands= signature_matr.flatMap(lambda x: LSH(x,bands, rows))

        reduce_lsh = Lsh_bands.groupByKey()
    #print(reduce_lsh)
        second_map_cleared = reduce_lsh.mapValues(list)
        second_map_filtered = second_map_cleared.filter(lambda x:len(x[1])>1) #.= ? hash bands what, key? (band, tuple)
        user_pairs_cands = second_map_filtered.flatMap(lambda x: list(combinations(x[1],2))).distinct().map(lambda x: small(x))
        #
        print(user_pairs_cands.collect())

        user_pairs_filt_jaccard = user_pairs_cands.map(lambda x:jaccard(x, user_bus_set)).filter(lambda x: x[2]>=0.01).map(lambda x:(x[0],x[1]))

        user_pairs = user_pairs_filt_jaccard.map(lambda x: (x[0],x[1], user_bus_set[x[0]].intersection(user_bus_set[x[1]])))
        user_pairs_filt = user_pairs.filter(lambda x: len(x[2])>=3)


        user_pearson = user_pairs_filt.map(lambda x: pearson_coeff(x, user_bus_group_star))

        user_pearson = user_pearson.filter(lambda x: x[2]>0).collect()

        return user_pearson




    #return model





if __name__ == '__main__':

    if True
        path = "../resource/asnlib/publicdata/"
        test_temp_path = path+"test_review.json"
        bus_path = path+"business.json"
        bus_avg_path = path+"business_avg.json"
        train_review_path = path+"train_review.json"
        user_path = path+"user.json"
        user_avg = path+"user_avg.json"

        train_review = train_review_path
        model_file = temp.json
        cf_type = ite


        conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory", "4g"))

        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")

        output = task3(sc,train_review, cf_type)
        #print(output)

        with open(model_file, 'w+') as file:

            if cf_type == "item_based":
                for row in output:

                    temp_dict = dict()

                    temp_dict['b1'] = row[0]

                    temp_dict['b2'] = row[1]

                    temp_dict['sim'] = row[2]

                    line = json.dumps(temp_dict)+"\n"

                    file.writelines(line)

            else:
                for row in output:

                    temp_dict = dict()

                    temp_dict['u1'] = row[0]

                    temp_dict['u2'] = row[1]

                    temp_dict['sim'] = row[2]

                    line = json.dumps(temp_dict)+"\n"

                    file.writelines(line)

            file.close()



        print(time.time()-start)
    else:
        print("Not Enough Number of Arguments")
