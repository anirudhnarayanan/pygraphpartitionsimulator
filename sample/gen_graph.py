#!/usr/bin/env python

import random 
from Vertex import Node
from Edge import Edge

class GenGraph:
    def __init__(self,v,e,pa,pb,pc,pd):
        self.vertices = 10000
        self.edges = 1200000
        self.pA = 0.45
        self.pB = 0.15
        self.pC = 0.15
        self.pD = 0.25
        self.edges_list = []
        self.edges_list_uniform = []
        self.set_of_vertices = {}


        
    def gengraph(self):
        #splitting now into quadrants based on possibilities
        quadA = self.pA
        quadB = quadA + self.pB
        quadC = quadB + self.pC
        quadD = 1
        edges_allocated = 0
        min_allocatable = 1000000
        while self.edges > edges_allocated:
            number_to_allocate = min(min_allocatable,self.edges)
            for i in range(self.vertices+1):
                """ This is the place, where the edges based on probability are scattered across the graph entirely"""
                row_start = 0
                col_start = 0
                row_end = self.vertices -1
                col_end = self.vertices -1
                fromids = []
                toids = []
                rand = random.uniform(0,1)
                while not row_start == row_end or not col_start == col_end:

                    if rand < quadA:
                        row_end = row_start + (row_end-row_start)/2
                        col_end = col_start + (col_end-col_start)/2

                    elif rand < quadB:
                        row_end = row_start + (row_end-row_start)/2
                        col_start = col_end - (col_end-col_start)/2

                    elif rand < quadC:
                        row_start = row_end - (row_end-row_start)/2
                        col_en = col_start + (col_end-col_start)/2

                    else:
                        row_start = row_end - (row_end - row_start)/2
                        col_start = col_end - (col_end - col_start)/2


                fromids[i] = col_start
                toids[i] = row_start

            addEdges(fromids,toids)
            edges_allocated = edges_allocated + number_to_allocate



    def addEdges(self,src, dest):
        for i in range(src):
            if not src[i] in self.set_of_vertices:
                a = Node(src[i])
                self.set_of_vertices[src[i]] = a

            else:
                a = self.set_of_vertices[src[i]]
            
            if not dest[i] in self.set_of_vertices:
                b = Node(dest[i])
                self.set_of_vertices[dest[i]] = b
            
            else:
                b = self.set_of_vertices[dest[i]]


            self.edges_list.append(a.getData(),b.getData())
            a.setEdge(b)

    def PutInFile(self,outfile):
        with open(outfile,"a") as writefile:
            for vertex in set_of_vertices.values():
                for dest_node in vertex.next.keys():
                    writefile.write(str(vertex.getData()) + " " + str(dest_node.getData())+"\n")



    def ReturnEdgesUniform(self):
        #different form
        self.edges_list_uniform = []
        for vertex in self.set_of_vertices.values():
            for dest_node in vertex.next.keys():
                temp_edge = Edge(vertex.getData(),dest_node.getData())
                self.edges_list_uniform.append(temp_edge)

    def ReturnEdges(self):
        return self.edges_list

                




            
            
            




