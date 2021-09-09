from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import json
import re
import collections
import copy

def task2(case,input_file,between_output,cum_out):
    raw_data = sc.textFile(input_file)
    head = raw_data.first()
    print(head)
    raw_data = raw_data.filter(lambda row: row!=head).map(lambda x: x.split(','))
#### this is data format 1, graph format
    if case == '1':
        edges = raw_data.map(lambda x: tuple(x)).collect()
        ## notes: flattened 
        vertices = raw_data.flatMap(lambda x: x).distinct().collect()

        adjacency = dict()
        for edge in edges:
            if edge[0] not in adjacency:
                adjacency[edge[0]] = set()
            if edge[1] not in adjacency:
                adjacency[edge[1]] = set()
            adjacency[edge[0]].add(edge[1])
            adjacency[edge[1]].add(edge[0])
        edges_length = len(edges)
    else:
        vertices = set()
        edges = set()
        user_states = raw_data.map(lambda x: (x[0],[x[1]])).reduceByKey(lambda a,b:a+b).collectAsMap()
        for key1 in user_states.keys():
            for key2 in user_states.keys():
                if key1!=key2:
                    jaccard =  len(set(user_states[key1]).intersection(set(user_states[key2])))/len(set(user_states[key1]).union(set(user_states[key2])))
                    if jaccard >= 0.5:
                        vertices.add(key1)
                        vertices.add(key2)
                        edges.add(tuple([key1,key2]))
        adjacency = dict()
        for edge in edges:
            if edge[0] not in adjacency:
                adjacency[edge[0]] = set()
            if edge[1] not in adjacency:
                adjacency[edge[1]] = set()
            
            adjacency[edge[0]].add(edge[1])
            adjacency[edge[1]].add(edge[0])
        edges_length = len(edges)

    between_vals = betweenness(vertices, adjacency)

    with open(between_output,"w") as file:
        for val_bet in between_vals:
            line = str(val_bet)[1:-1]+'\n'
            file.writelines(line)
        file.close()

    valss=ex_community(vertices, adjacency,between_vals, edges_length)

    with open(cum_out,"w") as file:
        #print(valss)
        for val_bett in valss:
            line = str(val_bett)[1:-1]+'\n'
            file.writelines(line)
        file.close()

    

def bfs(adjacency, root):
    visited = []
    level_dict = dict()
    parent_tree_dictionary = dict()
    bfs_path = dict()

    que = [root]
    my_set_track = set()
    level_dict[root] = 0
    bfs_path[root] = 1
    level_elements = dict()
    level_elements[1] = root
    
    

    while len(que) != 0:
        current = que.pop(0)
        visited.append(current)
        children = adjacency[current]
        for child in children:
            if child not in my_set_track:
                level_dict[child] = level_dict[current]+1+1
                #print(level_elements)
                new_l_temp = level_dict[current]+1+1
                #print(new_l_temp)
                if new_l_temp not in level_elements:
                    level_elements[new_l_temp] = set()
                #print(level_elements)
                level_elements[new_l_temp].add(child)
                #print(level_elements)
                que.append(child)
                if child not in parent_tree_dictionary:
                    parent_tree_dictionary[child] = set()
                parent_tree_dictionary[child].add(current)
                if child not in bfs_path:
                    bfs_path[child] = 0
                bfs_path[child] += bfs_path[current]
                my_set_track.add(child)
            else:
                if level_dict[child] == level_dict[current] +1:
                    if child not in parent_tree_dictionary:
                        parent_tree_dictionary[child] = set()
                    parent_tree_dictionary[child].add(current)
                    if child not in bfs_path:
                        bfs_path[child] = 0
                    bfs_path[child] += bfs_path[current]


    #####
    edge_sorr  = dict()
    verdict = dict()
    sorted_leys = sorted(list(level_elements.keys()),reverse = True)
    for b_el in sorted_leys:
        # alert level dict instead of reverse bfq
        #print(list(level_elements[b_el]), 'here', isinstance(level_elements[b_el],str), len(level_elements[b_el]))
        if isinstance(level_elements[b_el],str):
            level_elements[b_el] = list(tuple([level_elements[b_el]]))
        for node_b in level_elements[b_el]:
            #print(node_b,'here 2')
            #print assumption below
            if node_b not in parent_tree_dictionary:
                parent_tree_dictionary[node_b] = set()
            paren_s = parent_tree_dictionary[node_b]
            for par in paren_s:
                if node_b not in verdict:
                    verdict[node_b] = 1                    
                val_compute = verdict[node_b]*float(bfs_path[par]/bfs_path[node_b])
                if par not in verdict:
                    verdict[par] = 1
                verdict[par] =verdict[par]+ val_compute
                edge_temp = (min(node_b,par ), max(node_b,par ))
                if edge_temp not in edge_sorr:
                    edge_sorr[edge_temp] = 0
                edge_sorr[edge_temp] = edge_sorr[edge_temp]+val_compute
    
                

    
    return edge_sorr


def betweenness(vertices, adjacency):

    betweeen_dict= dict()
    for vertex in vertices:
        edge_sorr = bfs(adjacency, vertex)

        for edge_tuple in edge_sorr:
            if edge_tuple not in betweeen_dict:
                betweeen_dict[edge_tuple] = 0 
            betweeen_dict[edge_tuple] += (edge_sorr[edge_tuple]/2)
    
    sorted_items = sorted(betweeen_dict.items(), key=lambda item: item[1], reverse =True)
    return sorted_items                   


def get_one_community(adjac,verrte):
    one_community = set()
    que = []
    que.append(verrte)

    while len(que)!=0:
        ex_node = que.pop(0)
        one_community.add(ex_node)
        children = adjac[ex_node]
        for child in children:
            if child not in one_community:
                que.append(child)
        return sorted(list(one_community))

def get_all_communities(adjac,clean_edge):
    new_adjac = copy.deepcopy(adjac)
    # modify adjac
    new_adjac[clean_edge[0]].remove(clean_edge[1])
    new_adjac[clean_edge[1]].remove(clean_edge[0])

    vers = adjac.keys()
    vers = set(adjac.keys())

    all_communities = list()
    # alert vers.pop() ?
    for i in vers:
        all_communities.append(get_one_community(new_adjac,i))

    return all_communities

def modularity(all_comms,new_adj,edges_length):
    summ= 0
    for com in all_comms:
        if isinstance(com,str):
            com = [com]
        for ii in com:
            for jj in com:
                ki= len(new_adj[ii])
                kj = len(new_adj[jj])
                aij = 1 if ii in new_adj[jj] else 0

                summ+= aij - ((ki*kj)/2*edges_length)
    summ = summ/2*edges_length
    return summ 
    
    
    
def ex_community(vertices, adjacency,between_vals, edges_length):
    output = []
    new_adj= copy.deepcopy(adjacency)
    betw = betweenness(vertices, adjacency)
    maxx = modularity(vertices,adjacency, edges_length)
    while betw:
        #print('loop')
        clean_edge = betw[0][0]
        all_comms = get_all_communities(new_adj,clean_edge)
        print(all_comms)
        ### alert sets etc not used
        all_comms_temp = copy.deepcopy(all_comms)
        mod= modularity(all_comms_temp,new_adj,edges_length)
        if mod>maxx:
            maxx = mod
            output = copy.deepcopy(all_comms_temp)

        new_adj[clean_edge[0]].remove(clean_edge[1])
        new_adj[clean_edge[1]].remove(clean_edge[0])
        #print(new_adj)
        #print(vertices)
        betw = betweenness(vertices, new_adj)

    #assumption
    output.sort(key = lambda elem: (len(elem), elem[0]))
    return output

if __name__ == '__main__':
    if len(sys.argv[1:]) == 4:
        case = sys.argv[1]
        input_file = sys.argv[2]
        between_output = sys.argv[3]
        cum_out = sys.argv[4]

        conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("My app")
         .set("spark.executor.memory", "4g"))

        sc = SparkContext(conf = conf)
        sc.setLogLevel("ERROR")

        task2(case,input_file,between_output,cum_out)

    else:
        print("Not Enough Number of Arguments")

#spark-submit 1 sample_user_state.csv bet.txt cum.txt 
