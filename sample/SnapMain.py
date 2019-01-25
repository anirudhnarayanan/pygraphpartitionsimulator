#!/usr/bin/env python
#Main SNAP file. Contains code to call offline and online partitioning algorithms for testing in Python to facilitate use of TensorFlow

import sys
import math
from Edge import Edge
from ThreePhase import ThreePhase
from Vertex import Node
from Fennel import Fennel
from hash import EdgeCutHashing
from gen_graph import GenGraph
from Directed_Graph import gen_directed


def snap_main(*args):
    direc = args[1]
    file_name = args[2]
    gtype = args[3]
    op = args[4]
    start_thres = int(args[5])

    original_graph_file = direc + "/" + file_name    #Original graph file path
    metis_graph_file = original_graph_file + ".metis"
    metis_graph_32partition_file = metis_graph_file + ".part.32"


    #Creating array of Edges, Possible Linked List implementation

    print "here before gtype check"
    print gtype.lower()

    if gtype.lower() == "undirected":
        #Create Undirected Generated Graph, possbily using multi linked list
        #generated_nodes
        pass

    elif gtype.lower() == "directed":
        generated = gen_directed(original_graph_file) #Generated Edges
        print "reaches directed"
        #print generated
        #Create Generated Graph, possbily using multi linked list
        pass
    
    else:
        print "Please Specify Directed or Undirected"
        return 0

    
    csr = {}
    reverse_csr = {}

    max_vid = 0
    numVertices = 0
    real_edge_num = 0;

    """
    temp_gen_nodes = generated_nodes.copy() #for verification and removal temporarily
    CSR and Reverse CSR Maintain nodes in a way, where it is a hashed table containing the value as a set of all it's edges (destination vertices). Also this is bidirectiona.
    
    for node in temp_generated_nodes:
        temp_node = node.getData()
        if node.getData() > max_vid:
            max_vid = temp_node
        
        real_edge_num = real_edge_num + 1

        if not temp_node in csr.keys():
            csr[temp_node] = set()

        if not temp_node in reverse_csr.keys():
            reverse_csr[temp_node] = set()

        for dest_node in node.next.keys():
            csr[temp_node].add(dest_node)
    """
    for edge in generated:
        print "here"
        if edge.src == edge.dest: #if the edge points to itself, then it isn't considered
            print "equal"
            continue
        
        if max(edge.src,edge.dest) > max_vid:
            print "max vid exceeded"
            max_vid = max(edge.src,edge.dest)

        if not edge.src in csr:
            csr[edge.src] = set()
        if not edge.dest in csr:
            csr[edge.dest] = set()

        if not edge.src in reverse_csr:
            reverse_csr[edge.src] = set()
        if not edge.dest in reverse_csr:
            reverse_csr[edge.dest] = set()

        real_edge_num +=1

        

    print "CSR KEY SIZE: " + str(len(csr.keys()))


    outputs = [None]*(max_vid+1)

    for node in range(max_vid+1):
        if node in csr.keys():
            outputs[node] = ""
            dest_nodes = csr[node]

            for item in dest_nodes:
                outputs[node] = str(item) + " "
        
        if node in reverse_csr.keys():
            if outputs[node] is not None:
                outputs[node] = ""
            dest_nodes = csr[node]

            for item in dest_nodes:
                outputs[node] = str(item) + " "


    numVertices = max_vid + 1

    print numVertices



    
    print op
    if op.lower() == "gengraph":
        vertices = 10000
        edges = 1200000
        pA = 0.45
        pB = 0.15
        pC = 0.15
        pD = 0.25

        generator = GenGraph(pA,pB,pC,pD,vertices,edges)
        generator.gengraph()
        print "here"

        with open(original_graph_file,"w") as gengraph:
            for edge in edges_list:
                edgenow = edge.src + " " + e.dst + "\n"


    if op.lower() == "iogp":
        fullset = set()
        edgeset = []
        rev_edgeset = []
        for edge in generated:
            edgeset.append(edge)

        rev_edgeset = edgeset[:]
        rev_edgeset.reverse()
        fullset = set(edgeset + rev_edgeset)

        ThreePhase.workload_run(fullset,32)

    if op.lower() == "iogpperf":
        fullset = set()
        edgeset = []
        rev_edgeset = []
        for edge in generated:
            edgeset.append(edge)

        rev_edgeset = edgeset[:]
        rev_edgeset.reverse()
        fullset = set(edgeset + rev_edgeset)


        for thrs in range(1,52,5):
            reassigned = 0
            for node in csr:
                value = csr[node]
                k = float(len(value)/float(thrs))
                reassigned += max((math.log(k)/math.log(2)),0)
                
            print( "threshold " + thrs + " reassign " + reassigned)
            Threephase.workload_run_threshold(fullset,thrs)
                

        Threephase.workload_run(fullset,32)


    if op.lower() == "fennel":
        fullset = set(generated)
        f = Fennel(csr,reverse_csr,edges,32)
        f.workload_run_s()

    if op.lower() == "hash":
        fullset = set(generated)
        EdgeCutHashing.execute_partition()
    
    if op.lower() == "metis":
        with open(metis_graph_file,"w") as fill:
            fill.write(numVertices + " " + real_edge_num + "\n")
        
            for i in range(max_vid + 1):
                if not outputs[i] == None:
                    fill.write(str(outputs[i]).strip() + "\n")
                else:
                    fill.write("\n")

    
    if op.lower() == "metis-count":
        metis_results = metis_graph_32partition_file
        with open(metis_results,"r") as file1:
            line = ""
            i =0
            location = {}
            counts = []
            for j in range(32):
                counts[j] = 0

            content = file1.readlines(32)
            
            
            for line in content:
                location[i] = int(line)
                counts[int(line)] +=1

            print " " 
            for i in range(32):
                print counts[i] + " " 

            fullset = set(generated)
            numEdges = len(fullset)

            total_cut = 0

            for e in fullset:
                src = e.src
                dest = e.dest

                src_loc = locations.get(src)
                dest_loc = locations.get(dest)

                if not src_loc == dst_loc:
                    total_cut += 1


            print "discl.ttu.edu.MetisMain Total Cuts: " + total_cut + " Percent: " + float (total_cut) / float( numEdges)


        
        if op.lower() == "tmp":
            for threshold in range(1,52,5):
                reassigned = 0
                for node in csr:
                    k = float(len(csr[node]))/float(threshold)

                    reassigned += max((math.log(k)/math.log(2)),0) 


                print( "threshold " + thrs + " reassign " + reassigned)
                

        if op.lower() == "dist":
            for threshold in range(1,52,5):
                vertex_number =0

                for node in csr:
                    if len(csr[node]) >= threshold:
                        vertex_number +=1
                

                print ("threshold " + threshold + " over: " + vertex_number)












                


                     
                
        










         
        
        


            
        


    

    


    






if __name__ == "__main__":
    if len(sys.argv) < 6:
        """Argument Check, if not enough arguements print Usage"""
        print "Usage %s <dir> <filename> <type> <operation> <initial threshold>"%sys.argv[0]
        sys.exit()

    print type(sys.argv)
    snap_main(*sys.argv)


    

    
