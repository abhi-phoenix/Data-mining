import sys
import json
import random
import math
import csv
import numpy as np
import os

def dist(arr_x,arr_y,ll, type_dist):
    if type_dist == "euc":
        ss = sum([(x-y)**2 for (x,y) in zip(arr_x,arr_y)])
        #print(ss)
        ss = math.sqrt(ss)
        return ss
    elif type_dist == "maha":
        ss = sum([((x-y)/ll_i)**2 for (x,y,ll_i) in zip(arr_x,arr_y,ll)])
        ss = math.sqrt(ss)
        return ss
"""
class KMeans:
    def __init__(self,num):
        self.cluster_size = num
    def fit(self):
"""
def assignment(centers_init,aa):
    clusters = dict()
    points = dict()
    for cnt, i in enumerate(aa):
        distances = []
        for center in centers_init:
            #print(center,i, dist(center[1:],i[1:],0, "euc"))
            distances.append(dist(center,i[1:],0, "euc"))
        #print(distances)
        clus_index = distances.index(min(distances))
        #print(min(distances))
        points[cnt] = clus_index
        if clus_index not in clusters:
            clusters[clus_index] = []
        clusters[clus_index].append(i)
    return clusters, points

def centroid(clus_vals,aa):
    centers_ls = []
    for i in clus_vals.keys():
        clus_list = clus_vals[i]
        #print('err 1',clus_list)
        #print('err 2',clus_list[:3])
        clus_list = [x[1:] for x in clus_list]
        #print('err 3',len(clus_list),clus_list, '\n')
        centers = np.sum(clus_list,0)
        centers = centers/len(clus_list)
        #print('err 4 ',centers)
    #print(assigned)
        centers_ls.append(centers)

    #centers_ls = []
    #for i in clus_vals.keys():
#        clus_list = clus_vals[i]
#        clus_list = [x[1:] for x in clus_list]
        
    #print(centers_ls)
    return centers_ls

def points_differ(points_clus, points_clus2):
    #points_clus = dict(sorted(points_clus.items()))
    #points_clus2 = dict(sorted(points_clus2.items()))
    
    for i in points_clus:
        val = points_clus[i]
        val2 = points_clus2[i]
        if val != val2:
            return False
    return True
                
    
def task1(input_path, n_clusters, output_path, max_iters):
    iters_cur = 0 
    aa = []
    with open(input_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            row = [float(i) for i in row]
            aa.append(row)

    ap = random.sample(list(range(len(aa))),n_clusters)
    centers_init = []
    for i in ap:
        centers_init.append(aa[i][1:])
    clus_vals, points_clus = assignment(centers_init,aa)
    while iters_cur<=max_iters:
        
        compute_centroids = centroid(clus_vals, aa)
        clus_vals2, points_clus2 = assignment(compute_centroids,aa)
        boolean = points_differ(points_clus, points_clus2)
        if boolean ==True:
            break
        clus_vals = clus_vals2
        points_clus = points_clus2
        iters_cur+=1
        #print('this',iters_cur)
        print(points_clus)
    
    
    return points_clus2



if __name__ == '__main__':
    if len(sys.argv[1:]) == 4:
        input_path = sys.argv[1]
        n_clusters= int(sys.argv[2])
        output_path = sys.argv[3]
        output_path2 = sys.argv[4]
        max_iters = 10
        output = task1(input_path, n_clusters, output_path, max_iters) 
        
        with open(output_path, 'w') as outfile:
            json.dump(output, outfile)
        #task2(sc,business_file_path)

    else:
        print("Not Enough Number of Arguments")
