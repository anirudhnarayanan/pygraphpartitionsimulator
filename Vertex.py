#!/usr/bin/env python


class Node:
    def __init__(self,initdata):
        self.data = initdata
        self.next = {}
        self.next_counter = 0

    def getData(self):
        return self.data

    def getNext(self): #sort of like the KEY ID or graph ID
        return self.next

    def setData(self,newdata):
        self.data = newdata

    def setEdge(self,new_node,edge_connector = "default"):
        self.next[edge_connector] = new_node
        self.next_counter = self.next_counter + 1

    def number_of_edges(self):
        return self.next_counter

    def hasNext(self):
        return self.next_counter>0

        
