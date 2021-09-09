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
from graphframes import GraphFrame
from pyspark.sql import SQLContext, Row
import os

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")
start = time.time()


def task1(sc,filter_threshold,input_file):
    sqlcontext = SQLContext(sc)
      
    textRDD = sc.textFile(input_file)
    header = textRDD.first()
    #print(header)
    textRDD = textRDD.filter(lambda x: x!=header)
    user_business = textRDD.map(lambda x: tuple(x.split(',')))
    user_business = user_business.map(lambda x: (x[0],[x[1]]))
    group_user = user_business.reduceByKey(lambda a,b:a+b) #user_business.groupByKey().mapValues(list)
    group_user_dict = group_user.collectAsMap()

    users = user_business.map(lambda x: x[0]).distinct()
    user_nodes = users.map(lambda x: Row(id =x)).collect()
    user_nodes_df = sqlcontext.createDataFrame(user_nodes,["id"])
    
    users_pairs = list(combinations(users.collect(),2))
    users_pairs_rdd = sc.parallelize(users_pairs)
    """
    set_full = set()
    for i in users_pairs:
        if len(set(group_user_dict[i[0]]).intersection(set(group_user_dict[i[1]])))>=7:
            set_full.add(i)
    print('note',len(set_full))
    """
    users_pairs_rdd_filt = users_pairs_rdd.filter(lambda x: len(set(group_user_dict[x[0]]).intersection(set(group_user_dict[x[1]])))>=filter_threshold).map(lambda x: Row(src =x[0],dst = x[1])).collect()
    #print(users_pairs_rdd_filt)
    edges_df = sqlcontext.createDataFrame(users_pairs_rdd_filt,["src","dst"])

    #print(user_nodes_df,'ipo')
    #print(edges_df,'edges e')
    graphh = GraphFrame(user_nodes_df,edges_df).labelPropagation(maxIter=5).rdd.coalesce(1)
    #print(graphh.first())
    #graph = graphh.map(lambda x: (x[1],x[0])).groupByKey().mapValues(list).map(lambda x: (len(x[1]),x[0],sorted(x[1]))).sortByKey(ascending=True) #.groupByKey()collect())
    #return graph.collect()
    #print(graph.collect())
    
    graph = graphh.collect()
    labelled_dict = dict()
    for i in graph:
        if i[1] not in labelled_dict:
            labelled_dict[i[1]] = [i[0]]
        else:
            print('jiop')
            labelled_dict[i[1]].append(i[0])
    #print(labelled_dict)

    sort_orders = sorted(labelled_dict.items(), key=lambda x: len(x[1]), reverse=False)
    print(sort_orders)
    return sort_orders
             
        
        


if __name__ == '__main__':
    if len(sys.argv[1:]) == 3:
        filter_threshold = int(sys.argv[1])
        input_file = sys.argv[2]
        community_output = sys.argv[3]

        conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))

        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")

        output = task1(sc,filter_threshold,input_file)

        
        with open(community_output, 'w') as file:
            for row in output:
                line = str(sorted(row[1]))[1:-1]+"\n"
                file.writelines(line)
            file.close()
        print(time.time()-start)

    else:
        print("Not Enough Number of Arguments")


# spark-submit task1.py 2 sample_user_state.csv output.txt
