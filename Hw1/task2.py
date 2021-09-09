from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import json
import re
import collections

def task2(sc,path,path2, use,n_categories):
    output = dict()

    if use == "spark":
        res = dict()
        textRDD = sc.textFile(path)
        RDD_dictionary = textRDD.map(json.loads)

        textRDD2 = sc.textFile(path2)
        RDD_dictionary2 = textRDD2.map(json.loads)

        bus = RDD_dictionary2.map(lambda business: (business['business_id'],business['categories']))
        rev = RDD_dictionary.map(lambda rev: (rev['business_id'],rev['stars']))

        def temp(curr):
            l = []
            for val in curr[0].split(","):
                l.append((val, curr[1]))
            return l

    #IMP    #merg = bus.join(rev).map(lambda row: (row[1][0],row[1][1])).filter(lambda row: row[0]!= None and row[1]!= None).map(lambda row: temp(row)).flatMap(lambda row: row).map(lambda row: (row[0].strip(),row[1]))
        #merg = bus.join(rev).map(lambda row: (row[1][0],row[1][1])).filter(lambda row: row[0]!= None and row[1]!= None).flatMapValues(lambda row: (row[1],row[0].split(','))) #.map(lambda row: (row[1].strip(),row[0]))
        tremp =  bus.join(rev).map(lambda row: (row[1][0],row[1][1])).filter(lambda row: row[0]!= None and row[1]!= None).map(lambda row: temp(row)).flatMap(lambda row: row).map(lambda row: (row[0].strip(),(row[1],1)))
        # here below
        berg = tremp.reduceByKey(lambda a, b: (a[0]+b[0],a[1]+b[1]))

        ttremp = berg.map(lambda x: (x[0],x[1][0]/x[1][1]))
        #print(ttremp.collect())
        ## here below

        ## CHECK ALERT
        ans = ttremp.sortBy(lambda x: (-x[1],x[0])).take(n_categories)  #.top(n_categories)
        #print('is it',ans)
        #ans_list = [[i[1],i[0]] for i in ans]
        #print('heret',ans_list)
        ## not indexing in the dictionary
        res["result"] = [[i[0],i[1]] for i in ans]

        return res
    else:
        res = dict()
        data1 = [json.loads(line) for line in open(path, 'r')]
        data2 = [json.loads(line) for line in open(path2, 'r')]
        data_dict = dict()
        data2_dict = dict()
        merge = dict()
        excess_dict = dict()
        for i in data1:
            if i['business_id'] in data_dict.keys():
                #print('tadatadada')
                if i['business_id'] in excess_dict:
                    excess_dict[i['business_id']].append(i['stars'])
                else:
                    excess_dict[i['business_id']] = [i['stars']]
            else:
                data_dict[i['business_id']] = i['stars']


        for j in data2:
            if j['categories'] != None:
                liss = j['categories'] #[m.strip() for m in j['categories'].split(',')]
                if j['business_id'] in data2_dict:
                    print('supermantadattada')
                else:
                    data2_dict[j['business_id']] = liss


            #for word in lis:
                #### wrong overwriting keys
            #    data2_dict[j['business_id']] = word
        #print(data2_dict, '\n\n',data_dict)
        excess_tuples = []
        for key in excess_dict.keys():
            if key in data2_dict.keys():
                list_words = [m.strip() for m in data2_dict[key].split(',')]
                for ex_word in list_words:
                    for ex_value in excess_dict[key]:
                        excess_tuples.append([ex_word,ex_value])

        #print('debug excess', excess_tuples)


        trash = dict()
        for key in data2_dict.keys():
            if key in data_dict.keys():
                if data2_dict[key] not in merge:
                    merge[data2_dict[key]] = data_dict[key]
                else:
                    trash[data2_dict[key]] = data_dict[key]
        merge_words = dict()
        temp = []
        summo = 0
        countrrr = 0
        for d_key in merge.keys():
            d_keyy = [m.strip() for m in d_key.split(',')]
            for word in d_keyy:
                if word == 'Product Design':
                #    temp.append(word)
                    summo+= merge[d_key]
                    countrrr +=1
                if word in merge_words.keys():
                    merge_words[word][0] += float(merge[d_key])
                    merge_words[word][1] += 1

                else:
                    merge_words[word] = [merge[d_key],1]
###
        #print('big_shit',merge_words)
        for dd_key in trash.keys():
            dd_keyy = [m.strip() for m in dd_key.split(',')]
            for wordd in dd_keyy:
                if wordd in merge_words.keys():
                    #print(merge_words[wordd], 'hyt',dd_key)
                    merge_words[wordd][0]+= trash[dd_key]
                    merge_words[wordd][1] += 1
### excess keys merge_words
        for ex_tuple in excess_tuples:
            merge_words[ex_tuple[0]][0]+= ex_tuple[1]
            merge_words[ex_tuple[0]][1] += 1


#
        for key_avg in merge_words.keys():
            merge_words[key_avg] = merge_words[key_avg][0]/float(merge_words[key_avg][1])
        #print(merge_words,'jlsdnfhkj')
        ## group by keys and take average
        merge_words = [[k,v] for k,v in merge_words.items()]
        ordred = sorted(merge_words, key= lambda x: (-x[1],x[0]))
        #countt = 0
        #tops_vals = []
        #print(ordred[:n_categories])
        #for top_ele in ordred:
            #print(top_ele,countt)
#            tops_vals.append([top_ele[0],top_ele[1]])
#            if countt == n_categories-1:
#                break
#            countt+=1
#        print(tops_vals)
        res["result"] = ordred[:n_categories]
        print(res)
        #print(countrrr,summo)
        #print(tops_vals)
        #return res
        #top_n = list(merge_words.keys()).sort()[:n_categories]
        #top_n_words = [merge_words[u] for u in top_n]
        #print(top_n_words)








        return res



if __name__ == '__main__':
    if len(sys.argv[1:]) == 5:
        review_file_path = sys.argv[1]
        business_file_path = sys.argv[2]
        output_file_path = sys.argv[3]
        spark_use = sys.argv[4]
        n_categories = int(sys.argv[5])

        conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))

        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")




#        review_file_path = './review.json'
#        business_file_path='./business.json'
#        n_categories = 20
#        spark_use = "no_spark"
        output = task2(sc,review_file_path, business_file_path, spark_use,n_categories )

        with open(output_file_path, 'w') as outfile:

            json.dump(output, outfile)
        #task2(sc,business_file_path)

    else:
        print("Not Enough Number of Arguments")

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
