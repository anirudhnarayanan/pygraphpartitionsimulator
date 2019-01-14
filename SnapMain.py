#Main SNAP file. Contains code to call offline and online partitioning algorithms for testing in Python to facilitate use of TensorFlow

import sys

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

    if gtype.lower() == "undirected":
        #Create Undirected Generated Graph, possbily using multi linked list
        #generated_nodes
        pass

    elif gtype.lower() == "directed":
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

    temp_gen_nodes = generated_nodes.copy() #for verification and removal temporarily
    """CSR and Reverse CSR Maintain nodes in a way, where it is a hashed table containing the value as a set of all it's edges (destination vertices). Also this is bidirectiona."""
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
        

    print "CSR KEY SIZE: " + len(csr.keys())


    outputs = [None]*max_vid+1

    for node in range(max_vid+1):
        if node in csr.keys():
            outputs[i] = ""
            dest_nodes = csr[node]

            for item in dest_nodes:
                outputs[i] = str(item) + " "
        
        if node in reverse_csr.keys():
            if outputs[i] is not None:
                outputs[i] = ""
            dest_nodes = csr[node]

            for item in dest_nodes:
                outputs[i] = str(item) + " "


    numVertices = max_vid + 1
        


            
        


    

    


    






if __name__ == "__main__":
    if len(sys.argv) < 6:
        """Argument Check, if not enough arguements print Usage"""
        print "Usage %s <dir> <filename> <type> <operation> <initial threshold>"%sys.argv[0]
        sys.exit()
    

    
