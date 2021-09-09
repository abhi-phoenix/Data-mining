from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import json
import re
import time
from itertools import combinations

start = time.time()
def proc_partition(partition,sup_thres):
    diction = dict()
    set_cads = set()
    new_partition = list(partition)
    #yield('data pur',new_partition)
    for pair in new_partition:
        for id_p in pair[1]:
            if id_p not in diction:
                diction[id_p] = 1
            else:
                diction[id_p] += 1


#                if diction[id_p] >= sup_thres:
                    #yield (id_p,1)
#                    set_cads.add(diction[id_p])
#                else:
#                    pass
#            else:
#                if diction[id_p] < sup_thres:
#                    diction[id_p] += 1
#                else:
#                    if diction[id_p] not in set_cads:
#                        set_cads.add(diction[id_p])
                        #yield (id_p,1)

    count = 1
    Cads = list(diction.keys()) #sorted(diction.keys())
    #yield (str(count)+'_c',Cads)
    freq = [key for key,value in diction.items() if value >= sup_thres]
#J#    for jkpp in freq:
#J#        yield(jkpp,1)
#    freq = sorted(freq)
    if len(freq) != 0:
        yield (('uni',count),freq)

    while len(freq)!= 0 :

        count += 1
        if count == 2:
            pos_cads = list(combinations(freq,count))
            pos_cads = [tuple(sorted(i)) for i in pos_cads]

        else:
            # ALERT set
            ls = set()
            for ii in range(len(freq)):
                for jj in range(ii+1,len(freq)):
                    if len(set(freq[ii]).union(set(freq[jj])))==count:
                        elem = tuple(sorted(tuple(set(freq[ii]).union(set(freq[jj])))))
                        ls.add(elem)
                        ###if elem not in ls:
###                            ls[elem] = 1
            #pos_cads = list({list(set(freq[i]).union(set(freq[j]))) for i in range(len(freq)) for j in range(i+1,len(freq)) if len(set(freq[i]).union(set(freq[j])))==count})
            #yield('posp',ls)
            pos_cads = list(ls)
        freq = dict()
        Cads = dict()
        #yield('pos cads',pos_cads)
        for each_basket in new_partition:
            #yield('debug basket 2',each_basket)
            for each_pos_pair in pos_cads:
                # ALERT
                #yield('debug pos 2',each_pos_pair)
                if set(each_pos_pair).issubset(set(each_basket[1])):
                    if each_pos_pair in Cads:
                        Cads[each_pos_pair]+=1
                    else:
                        Cads[each_pos_pair]=1


        freq = [key for key,value in Cads.items() if value >= sup_thres]
        #J#for jkp in freq:
#J#            yield(jkp,1)
#        freq = sorted(freq)
        if len(freq) != 0:
            yield (('uni',count),freq)
#        Cads = sorted(Cads.keys())

#        yield (str(count)+'_c',list(Cads.keys()))

def write_output(file,cands, freqs):
    with open(file,'w') as file_pointer:
        string_write = "Candidates:\n"
        separator = ','
        for i in cands:
            #print(i)
            if isinstance(i[1][0],tuple) ==False:
                #print(','.join("('{}')".format(i) for i in i[1])
                string_write+=(separator.join("('{}')".format(ii) for ii in i[1] )+'\n\n')
            else:
                #new = [str(i) for i in i[1]]
                string_write+=(separator.join("{}".format(ii) for ii in i[1] )+'\n\n')
        string_write += "Frequent Itemsets:\n"
        for i in freqs:
            if i[0] == 1:
                string_write+=(separator.join("('{}')".format(ii[0]) for ii in i[1] )+'\n\n')
            else:
                string_write+=(separator.join("{}".format(ii) for ii in i[1] )+'\n\n')

        string_write= string_write[:-2]
        file_pointer.write(string_write)


    return 1

def proc_2(bal_chunk,cands_list):
    dic = dict()
    for basket in bal_chunk:
        for item in cands_list:
            #yield('debug',item, isinstance(item,tuple))
            if isinstance(item,tuple) == True:

                #yield('debug',set(item),set(basket[1]), set(item).issubset(set(basket[1])))
                if set(item).issubset(set(basket[1])):

                    if item in dic:
                        dic[item] +=1
                    else:
                        dic[item] = 1
            else:
                item = tuple([item])
                if set(item).issubset(set(basket[1])):

                    if item in dic:
                        dic[item] +=1
                    else:
                        dic[item] = 1


    for key,value in dic.items():
        yield (key,value)


def Task1(sc,filter_thres,support, input_file):

    textRDD = sc.textFile(input_file)
    file_header = textRDD.first()
    textRDD2 = textRDD.filter(lambda row: row!= file_header).filter(lambda row: row != None).map(lambda row: row.split(',')).filter(lambda listt: len(listt)==2)
    # removed distinct don't know
    textRDD3 = textRDD2.map(lambda row: (row[0],row[1])).distinct().groupByKey().map(lambda x: (x[0],list(x[1])))
    text_inter = textRDD3.filter(lambda row: len(row[1])>filter_thres)
    #print(text_inter.collect())

        #print(textRDD2.map(lambda row: (row[1],row[0])).reduceByKey(lambda a,b:a).collect())
    sup_thres = support/text_inter.getNumPartitions()
        #print('here',textRDD3.getNumPartitions() )
    chunk_process = text_inter.mapPartitions(lambda chunk: proc_partition(chunk,sup_thres))
    #if case_number == 1:
    #print(chunk_process.collect())
    #print(chunk_process.collect())
    reduced = chunk_process.reduceByKey(lambda a,b: sorted(set(a+b))) #.map(lambda x: (x[0],list(x[1])))
    cands_list = [i for group in reduced.collect() for i in group[1]]
    map_ph2 = textRDD3.mapPartitions(lambda bal_chunk: proc_2(bal_chunk,cands_list))
    #print(cands_list,'\n\n\n',map_ph2.collect())
    map_red2 = map_ph2.reduceByKey(lambda a,b: a+b)
    map_port_proc = map_red2.filter(lambda row: row[1]>=support)
    #print('\n',map_red2.collect())

    #print(sorted(map_port_proc.collect()))
#    map_port_proc_sorted = map_port_proc.map(lambda x: (len(x[0]),(x[0],x[1]))).groupByKey().map(lambda x: (x[0],sorted(list(x[1]))))
    map_port_proc_sorted = map_port_proc.map(lambda x: (len(x[0]),x[0])).groupByKey().map(lambda x: (x[0],sorted(list(x[1]))))

    #print('hehe 1\n2',sorted(reduced.collect()))
    #print('hehe', sorted(map_port_proc_sorted.collect()))
    return sorted(reduced.collect()), sorted(map_port_proc_sorted.collect())



if __name__ == '__main__':
    if len(sys.argv[1:]) == 4:
        filter_thres = int(sys.argv[1])
        support= int(sys.argv[2])
        input_file = sys.argv[3]
        output_file = sys.argv[4]

        conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))

        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")
        #spark = SparkSession(sc)


        # ALERT
        #input_file = 'small1.csv'
        cands_l,freq_l = Task1(sc,filter_thres,support, input_file)
        write_output(output_file,cands_l, freq_l)
        print("Duration: ","{:.2f}".format(time.time()-start))


    else:
        print("Not Enough Number of Arguments")
