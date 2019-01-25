#!/usr/bin/env python

from Vertex import Node




def gen_directed(path):
    """ reading from graph file and inputting node into multi linked list"""
    with open(path,"r") as graph_file:
        directed_data = graph_file.read()

    directed_data = directed_data.split("\n")

    print directed_data


    node_array = set()

    if len(directed_data) == 1:
        return 0

    for line in directed_data:
        if not line == "#":
            split_line = line.split("\\W+")
            node1 = Node(int(split_line[0]))
            node_array.add(node1)
            if not split_line[0] == split_line[1]:
                node2 = Node(int(split_line[1]))
                node1.setEdge(node2)
                node_array.add(node2)


            




                



    
            

