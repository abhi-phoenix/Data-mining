from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import json
import re
import time

start = time.time()
def task1(sc,path,stop_words_file, y, m , n):
    output = dict()

    textRDD = sc.textFile(path)
    RDD_dictionary = textRDD.map(json.loads)

    mapper = RDD_dictionary.map(lambda review: review['review_id']).filter(lambda review: review!=None).map(lambda review: (review,1))
#    reducer = mapper.reduceByKey(lambda a, b: a+b)
#    print('Oswald, total number of reviews',reducer.count())
    output['A'] = mapper.distinct().count()

    mapper2 = RDD_dictionary.map(lambda review: (review['date'].split('-')[0],review['review_id']))
#    reducer2 = mapper2.filter(lambda review: review[0]==y).map(lambda review: (review[1],1)).reduceByKey(lambda a, b: a+b)
#    print(mapper2.filter(lambda review: review[0]==y).collect()) #.distinct().count()
    count2_red = mapper2.filter(lambda review: review[0]==y).distinct().count()
    
    output['B']=count2_red
#    print(reducer2.collect()[0][1])
#    output['B']=reducer2


    mapper3 = RDD_dictionary.filter(lambda review: review['review_id']!=None and review['text']!=None ).map(lambda review: (review['business_id'],1)) # if review['review_id'] != None )
    output['C']=mapper3.distinct().count()

    mapper4 = RDD_dictionary.filter(lambda review: review['review_id']!=None and review['text']!=None and review['user_id']!=None).map(lambda review: (review['user_id'],1)) # if review['review_id'] != None )
    reducer4 = mapper4.reduceByKey(lambda a, b: a+b).sortBy(lambda x: (-x[1],x[0])).take(m)
    
#    top_em = [i[1] for i in reducer4.top(m)]
#    print('Timon&Pumba',top_em)
    output['D']=[[i[0],i[1]] for i in reducer4]
    


    punc = ['(', '[', ',', '.', '!', '?', ':', ';', ']', ')']
    rx = '[' + re.escape(''.join(punc)) + ']'       
    f = open(stop_words_file, "r")
    stop_words = set(f.read().splitlines())
    #print('here')
    maper6 = RDD_dictionary.flatMap(lambda line: re.sub(rx, '', re.sub(rx, '', line['text'])).split()).map(lambda word: word.lower())
    mapper6 = maper6.filter(lambda word: word not in stop_words).map(lambda x: (x,1))
    #print('here 2', mapper6.collect())
#    words_freq = mapper6.reduceByKey(lambda a, b: a+b).map(lambda var: (var[1],var[0])).sortByKey(ascending=False)
    words_freq = mapper6.reduceByKey(lambda a, b: a+b)
    #print('tang')
    time_sort = time.time()
    inter = words_freq.takeOrdered(n, key=lambda x: (-x[1],x[0])) #sortBy(lambda x: (-x[1],x[0])).take(n)
    #print('here 3', inter.collect())
    output['E']=[i[0] for i in inter]
    #print('sort time',time.time()-time_sort)
    #print('here 4', output)
    #print(inter)
    #print(words_freq.top(n))
    #print('total time',time.time()-start)

## work
#    top_em2 = [i[1] for i in words_freq.top(n)]
#    print('debug', top_em2)
#    output['D':top_em2]

    #mapper6 = mapper6.filter(lambda x: x[0] not in stop_words)
    #reducer6 = mapper6.reduceByKey(lambda a, b: a+b).collect()
    #print(reducer6.sortByKey(ascending=False).top(n))

    return output
    
    


if __name__ == '__main__':
    if len(sys.argv[1:]) == 6:
        review_file_path = sys.argv[1]
        output_file_path= sys.argv[2]
        stop_words_file = sys.argv[3]
        y = sys.argv[4]
        m = int(sys.argv[5])
        n = int(sys.argv[6])
        conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
        
        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")
        #spark = SparkSession(sc)


        output = task1(sc,review_file_path,stop_words_file, y, m , n)
        
        with open(output_file_path, 'w') as outfile:
            json.dump(output, outfile)
        #task2(sc,business_file_path)

    else:
        print("Not Enough Number of Arguments")

"""
spark-submit check_spark.py ./review.json output1.json stopwords 2007 10 20

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
