#!/usr/bin/env python
import sys

class Fennel:
    def __init__(self,csr,reverse_csr,eges,size):
        self.csr = csr
        self.revcsr = reverse_csr
        self.edges = edges
        self.cluster_size = size
        self.cluster_servers = [set() for i in range(cluster_size) ]
        self.location = {}

    def workload_run_save(self):
        multiple = 10

        for node in self.csr.keys():
            current_node = node
            forward_next = self.csr[node]
            reverse_next = self.revcsr[node]

            all_next = forward_next.union(reverse_next)
            
            target = 0
            score = sys.maxint
            for server in range(self.cluster_size):
                overlap = 0
                for next_node in all_next:
                    if next_node in cluster_servers[server]:
                        overlap = overlap+1

                    fennel_score = len(cluster_servers[i]) - overlap

                    if fennel_score < score:
                        score = fennel_score
                        target = server

            
            if current_node in location:
                location.pop(current_node,None)

            location[current_node] = target


        highest_weighing_server = -sys.maxint-1
        lowest_weighing_server = sys.maxint

        for part in self.cluster_servers:
            if len(part) < lowest_weighing_server:
                lowest_weighing_server = len(part)
            if len(part) > highest_weighing_server:
                highest_weighing_server = len(part)

        print "BALANCE CALC(DIFFERENCE): " + max_weight_of_server - min_weight_of_servers


        for edge in edges:
            src = edge.src
            dest = edge.dest

            if not src in location or not dest in location:
                continue
            
            if not location[src] == location[dest]:
                self.total_cut = self.total_cut+1


        print "Partition Algorithm Fennel Total Cuts: " + total_cut + "Percentage: "+ float(total_cut)/float(len(edges))





