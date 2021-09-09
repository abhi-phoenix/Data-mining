
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
from pyspark.streaming import StreamingContext
import csv

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
    

def task2(sc,port,output_file):
    
    def generate_hashes(x, hash_func_tuples,m):
        funcs_all = []
        
        for i in hash_func_tuples:
            #print(i[0], i[1], i[2])
            val  = ((i[0]*x+i[1])%i[2])%m
            bin_val = bin(val)[2:]
            #print(type(bin_val))
            funcs_all.append(bin_val)
        return funcs_all

    def floyt_pitas(time_val,data_r):
        stream_data = data_r
        global output_file
        global group_size
        global m
        global hash_functions_count
        global hash_func_tuples

        each_group_val = int(hash_functions_count/group_size)
        #print(each_group_val)
        #print(output_file, group_size, m)
        #print(stream_data)
        ## state rdd
        rdd_proc = stream_data.map(lambda x: json.loads(x.strip())["state"]).map(lambda x: (x,int(binascii.hexlify(x.encode('utf8')),16)))
        rdd_proc_hex = rdd_proc.map(lambda x: x[1]).collect()
        rdd_proc_state = len(set(rdd_proc.map(lambda x: x[0]).collect()))

        hash_bins = []
        for state_hex in rdd_proc_hex:
            hash_state_hex = generate_hashes(state_hex, hash_func_tuples,m)
            hash_bins.append(hash_state_hex)
        estimation = []
        for hash_index in range(hash_functions_count):
            #print(hash_bins)
            hash_lists = [hash_stat_hex[hash_index] for hash_stat_hex in hash_bins]
            #print(hash_lists)
            long_trailing_zeros = max([len(i)-len(i.rstrip("0")) for i in hash_lists])
            #print('long',long_trailing_zeros)
            estimation.append(2**long_trailing_zeros)
        #print(estimation)

        comb_estimate = []
        for group_index in range(group_size):
            summ= 0
            for group_val_index in range(each_group_val):
                temp = estimation[group_index*each_group_val+group_val_index]
                summ = summ+ temp
            temp2= summ/each_group_val
            comb_estimate.append(temp2)


        comb_estimate.sort()
        temp3 = group_size/2
        m_mid = comb_estimate[int(temp3)]
        
        
        #file = open(output_file, 'a')
        #print('this one',time.strftime("%Y-%m-%d %H:%M:%S"),rdd_proc_state, m_mid, type(time), type(rdd_proc_state), type(m_mid))
        #strr = str(time.strftime("%Y-%m-%d %H:%M:%S"))+','+str(rdd_proc_state)+','+str(m_mid)
        #print(strr)
        #file.write("\n"+strr)
        #file.close()
        roww_val = [str(time.strftime("%Y-%m-%d %H:%M:%S")),str(rdd_proc_state),str(m_mid) ]
        with open(output_file,'a') as file:
            csv_wr = csv.writer(file)
            csv_wr.writerow(roww_val)

        
    ssc=StreamingContext(sc , 5)
    stream = ssc.socketTextStream("localhost", port).window(30,10)
    stream.foreachRDD(floyt_pitas)
    ssc.start()
    ssc.awaitTermination()

    
    return 1
    


if __name__ == '__main__':
    if len(sys.argv[1:]) == 2:
        port = int(sys.argv[1])
        output_file = sys.argv[2]

        conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory", "4g"))

        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")

        #file = open(output_file, 'w')
        #file.write("Time,Gound Truth,Estimation")
        #file.close()
        
        headerr= ["Time","Ground Truth","Estimation"]
        with open(output_file,'a') as file:
            csv_wr = csv.writer(file)
            csv_wr.writerow(headerr)
            

        hash_functions_count = 16
        m = 36
        group_size = 2
        hash_func_tuples = hash_func_tuples(m, hash_functions_count)
        
        output = task2(sc,port,output_file)
        
        #print(time.time()-start)

    else:
        print("Not Enough Number of Arguments")
        
