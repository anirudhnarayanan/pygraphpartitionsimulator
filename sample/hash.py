#!/usr/bin/env python

class EdgeCutHashing:
    def __init__(self,edges,cluster_size):
        self.edges = edges
        self.cluster_size = cluster_size

    def execute_partition(self):
        total_cuts = 0
        highest_in_degree = 0
        highest_out_degree = 0

        degrees = {}


        for e in edges:
            degrees[e.src] = degrees.get(e.src,0) + 1  #for weight calculation

            if not e.src % self.cluster_size == e.dst %self.cluster_size:
                total_cuts = total_cuts + 1

        for d in degrees.values():
            if d > highest_out_degree:
                highest_out_degree = d

        print "TOTAL CUTS " + str(total_cuts) + " Percent: " + str(float(total_cuts/float(len(edges.keys() ))))







