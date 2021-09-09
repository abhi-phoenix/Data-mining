from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import json
import tweepy
import random
import time
import collections
import csv

start = time.time()

def isEnglish(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True
    
#isEnglish ss
class MyStreamListener(tweepy.StreamListener):
    sample_length = 100
    seq_num = 0
    hash_tags = []
    global output_file
    def on_status(self, status):
        hashes = status.entities["hashtags"]
        if len(hashes) >0:
            hashes_ls = [i['text'] for i in hashes]
            self.seq_num= self.seq_num+1
            if len(self.hash_tags) <self.sample_length:
                for has in hashes_ls:
                    self.hash_tags.append(has)
            else:
                for hashes_n_index in hashes_ls:
                    probability = random.randint(0,999999)%self.seq_num
                    if probability <100:
                        random_int = random.randint(0,999999)%sample_length
                        self.hash_tags[random_int] = hashes_n_index
                
            counts = collections.Counter(self.hash_tags)
            count_sor = sorted(counts.items(),key=lambda x: (-x[1],x[0]))
            
            #file = open(output_file, 'w')
            strr_he = "The number of tweets with tags from the beginning: "+str(self.seq_num)
            header = [strr_he]
            prev=-1
            count_p =0
            ans = []
            for ii,vv in count_sor:
                if prev !=vv and count_p>=3:
                    break
                if prev !=vv:
                    count_p+=1
                prev = vv
     #           ans.append([ii,str(vv)])
                strr2 = [ii+" : "+str(vv)]
                ans.append(strr2)

            with open(output_file,'a', newline = '') as file:
                csv_wr = csv.writer(file)
                csv_wr.writerow(header)
                #for iii in ans:
                csv_wr.writerows(ans)
                csv_wr.writerow([])
                                
            
                    
                
                
  #          file.close()
            
            #print(self.seq_num, ans, output_file)
            
    def on_error(self, status_code):
        if status_code == 420:
            print(status_code, "error: tweepy API")
            return False

    
def task3(sc, port,output_file, api_key,api_secret,access_token,access_secret):

    auth=tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token,access_secret)
    api= tweepy.API(auth)
    
    mylistener = MyStreamListener()
    stream = tweepy.Stream(auth=api.auth,listener=mylistener)
    #stream.filter(languages = ["en"])
    stream.filter(languages = ["en"], track = ["#"])
    #print(stream)

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

        
        
        api_key = "qYhYe8FhO5ZOKjhYP0YooivWm"
        api_secret = "rQbHqHPc4UWrdeBE2vWmwyR5XMThyr6a6LRuNa0jhD39SwsJ29"
        #bearer_token = "AAAAAAAAAAAAAAAAAAAAAIPzJQEAAAAA2DK3XdIuah2MgS2dgSXOCD4tmFM%3DcLTLHG3a7HylQBxdzKO0z6lMSuGIotDklQnuzkhxK0AVSSnOnH"
        access_token = "1323076081629368320-ky29YjRHYx8M8IYevCaYFoQa1lQEkj"
        access_secret = "cSFj2wLAFAklkE0DEwZFWPq54yX4PrJ8vMVzEUpfMEzEf"
        output = task3(sc, port,output_file, api_key,api_secret,access_token,access_secret )
        
        #print(time.time()-start)

    else:
        print("Not Enough Number of Arguments")
