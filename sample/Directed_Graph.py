#!/usr/bin/env python

from Vertex import Node
import re
from Edge import Edge




def gen_directed(path):
    """ reading from graph file and inputting node into multi linked list"""
    with open(path,"r") as graph_file:
        directed_data = graph_file.read()

    directed_data = directed_data.split("\n")

    directed_data = directed_data[:10000]

    #print directed_data


    node_array = set()

    #if len(directed_data) == 1:
    #    return 0

    
    #this loop also takes into consideration the data structure in the format of a node. But this isn't used, and the edge list is the part more predomenantly used .
    generated = []
    for line in directed_data:
        if not line == "#":
            #split_line = line.split("\\W+")
            split_line = re.split("\\W+",line)
            node1 = Node(int(split_line[0]))
            temp_src = int(split_line[0])
            node_array.add(node1)
            if not split_line[0] == split_line[1]:
                node2 = Node(int(split_line[1]))
                temp_dest = int(split_line[1])
                node1.setEdge(node2)
                node_array.add(node2)
                temp_edge = Edge(temp_src,temp_dest)
                #print temp_edge.src,temp_edge.dest
                generated.append(temp_edge)

    print "GENERATED EDGES HERE"
    #print generated
    return generated


            




                



    
            

