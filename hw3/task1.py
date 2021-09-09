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

def task1(sc,train_review):

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
    
    hash_functions_count = 40
    bands = 40
    rows = hash_functions_count/ bands
    
    
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

    

    
    #print(second_map.collect())
    ### 222
    

    
    
    #print(business_user_transform_char.collect())
    #print(hash_func_tuples(length_users_all, hash_functions_count))
    


            

    #print(dic_business, dic_user)

                    
                
                
            
        
        
    #signature_matr = business_user_transform_char.map(lambda x: (x[0],get_signature(x[1]
    

    


"""
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

"""

if __name__ == '__main__':
    if len(sys.argv[1:]) == 2:
        train_review = sys.argv[1]
        output_file = sys.argv[2]

        conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))

        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")

        output = task1(sc,train_review)

        with open(output_file, 'w+') as file:
            for row in output:
                temp_dict = dict()
                temp_dict['b1'] = row[0]
                temp_dict['b2'] = row[1]
                temp_dict['sim'] = row[2]
                
                line = json.dumps(temp_dict)+"\n"
                file.writelines(line)
            file.close()

        #with open(output_file_path, 'w') as outfile:
#            json.dump(output, outfile)
        #task2(sc,business_file_path)
        #print(time.time()-start)

    else:
        print("Not Enough Number of Arguments")


# spark-submit task1.py train_review.json pp.txt
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
