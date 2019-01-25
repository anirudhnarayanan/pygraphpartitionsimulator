#!/usr/bin/env python


class Edge:
    def __init__(self,src,dest):
        self.src = src
        self.dest = dest

    def toString(self):
        return "[" + str(self.src) + ", " + str(self.dst) + "]"
    
    def hashCode(self):
        prime = 31
        result = prime * result
        result = prime*result + dst
        result = prime*result + src
        return src
    

