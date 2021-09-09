from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import json
import csv

def csv_format(line):
  return ','.join(d for d in line)

def preprocess(file1,file2,sc):
    textRDD = sc.textFile(file1)
    RDD_dictionary = textRDD.map(json.loads)

    textRDD2 = sc.textFile(file2)
    RDD_dictionary2 = textRDD2.map(json.loads)

    business = RDD_dictionary2.map(lambda business: (business['business_id'],business['state'],business['stars'])).filter(lambda row: row[2]>=4.0).map(lambda row: (row[0],row[1]))
    review = RDD_dictionary.map(lambda rev: (rev['business_id'],rev['user_id'] ))
    
    merge = review.join(business).map(lambda row: row[1])
    #csv_process = merge.map(lambda x: csv_format(x))
    header = sc.parallelize([("user_id","state")])
    #print(header.collect(),csv_process.collect())
    file_comb = header.union(merge)
    values = file_comb.collect()
    
    with open('user_state.csv', 'w') as file:
      writer = csv.writer(file, delimiter=",")
      writer.writerows(values)
    #file_comb.repartition(1).saveAsTextFile('tmp/user_state.csv')
    return 1

        
 


if __name__ == '__main__':
    
    review_file = 'yelp_academic_dataset_review.json'
    business_file= 'yelp_academic_dataset_business.json'


    conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory",'1g'))
        
    sc = SparkContext(conf = conf)
    sc.setLogLevel("ERROR")
    
    output = preprocess(review_file,business_file,sc)
        
