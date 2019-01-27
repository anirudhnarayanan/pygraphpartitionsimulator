#!/usr/bin/env python


class Edge:
    def __init__(self,src,dest):
        self.src = src
        self.dest = dest

    def __str__(self):
        return "[" + str(self.src) + ", " + str(self.dest) + "]"

    def __eq__(self,other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return 0
    
    def hashCode(self):
        prime = 31
        result = prime * result
        result = prime*result + dest
        result = prime*result + src
        return src
    

