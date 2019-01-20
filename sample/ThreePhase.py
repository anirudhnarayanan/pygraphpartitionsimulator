#!/usr/bin/env python

from Edge import Edge

class ThreePhase:
    def __init__(self,index,cluster,num):
        self.loc = {} #location of a random node
        self.split = {} #check if the vertex has been split or not
        self.ra = {}  #Reassigned to a different spot from to 
        self.v = {}  #integer to edge list, which it is connected to 
        self.local_neighbors = {} #hash to show my nodes, and nodes which are NEAR ME
        self.pointed_to_me = {} #hash to show my node, and those nodes which are pointed to me

        self.cluster = cluster

        self.MAX_EDGES = 50000
        self.MAX_REASSIGN = 1
        self.reassigntimes = 0
        self.serverNumber = num  #number of nodes
        self.index = index

    def hash(vertex_id):
        return vertex_id%serverNumber   #number of nodes


    def insertV(vertex_id):
        self.v[vertex_id] = set()
        self.local_neighbors[vertex_id] = 0
        self.pointed_to_me[vertex_id] = set()

        self.ra[vertex_id] = 1
        self.loc[vertex_id] = index
        self.split[vertex_id] = False


    def insertE(int src, int dest):
        newEdge = Edge(src,dest)  #create the new edge first
        hash_src = hash(src)   #Hash and find a node for the vertex now

        hash_dest = hash(dest)

        dest_serv = cluster[hash_dest].loc[dest]
        
        if not src in cluster[dest_serv].local_neighbors:
            cluster[dest_serv].local_neighbors[src] = 0
        
        cluster[dest_serv].local_neighbors[src] +=1 #FIXME find a way to do an atomic increment
        
        #IMP the loc could be the same as the cluster.get , but they vary in that if the true location has/had been changed , then the record will only reflect in loc.
        if not dest in self.pointed_to_me:
            self.pointed_to_me[dest] = set()

        self.pointed_to_me[dest].add(src)


        #vertex has been split

        if cluster[hash_src].split[src] == True: #DOUBTS
            #Requested wrong server?
            if not hash[dest] == self.index:
                return 1

            else:
                if not src in v:
                    v[src] = set()

                v[src].add(newEdge)
                return 0


        #Vertex removed but client still requests old location

        if not cluster[hash_src].loc[src] == this.index:
            #requesting old node
            return -1-cluster[hash_src].loc[src]

        else:
            v[src].add(newEdge)
            return 0

        #REASSIGNING VERTICES
        if len(v.keys()) >= (MAX_REASSIGN*cluster[hash_src].ra[src]):
            cluster.get(hash_src).ra[src] = cluster.get(hash_src).ra[src]*2

            from_node = cluster.get(hash_src).loc.get(src)
            neighbors = cluster.get(from_node).v.get(src)
            max_server = from_server
            fennel_score = sys.maxint
            
            local_fennel = 0
            for part in cluster:
                neigh_factor = 0

                if src in part.local_neighbours:
                    neigh_factor = part.local_neighbours.get(src)  #FIXME Atomic integer needed

                local_fennel = len(part.v) - neigh_factor

                if local_fennel < fennel_score:
                    max_server = part.index
                    fennel_score = local_fennel


            if not max_server == from_server:
                mov_edges = cluster.get(from_server).v.get(src)
                cluster.get(from_server).v.pop(src)


                cluster.get(hash_src).loc[src] = max_server
                cluster.get(max_server).v[src] = mov_edges

                reassigntimes = reassigntimes + 1


                if src in cluster.get(from_server).pointed_to_me:
                    for vtmp in cluster.get(from_server).pointed_to_me.get(src):
                        if not vtmp in cluster.get(from_server).local_neighbors:
                            cluster.get(from_server).local_neighbors[vtmp] = 0 #FIXME ATOMIC
                        
                        cluster.get(from_server).local_neighbors[vtpm] -=1 #FIXME ATOMIC DECREMENT


                if src in cluster.get(max_server).pointed_to_me:
                    for vtmp in cluster.get(max_server).pointed_to_me.get(src):
                        if not vtmp in cluster.get(max_server).local_neighbors:
                            cluster.get(max_server).local_neighbors[vtmp] = 0 #FIXME ATOMIC
                        
                        cluster.get(from_server).local_neighbors[vtmp]+=1

                return -1-max_server

            return 0

        #CHECK SPLIT

        if len(v.get(src)) > MAX_EDGES:
            cluster.get(src).split[src] = True
            all_edges = self.v.get(src)

            rms = []

            for edge in all_edges:
                if not hash(edge.dest) == self.index:
                    rms.add(edge)

            for edge in rms:
                target = cluster.get(hash(edge.dest))
                target.insertE(edge.src,edge.dest)
                this.v.get(src).remove(edge)

            cluster.get(hash_src).loc[src] = hash_src

            return 1

        return 0


    def workload_run(edges,cluster_size):
        tota_cut = 0
        total_reassign = 0
        highest_out_degree = 0
        highest_in_degree = 0

        insertedV = []
        splitV = []
        location = {}

        cluster = []

        #INITIALIZE CLUSTERS
        for i in range(cluster_size):
            cluster.append(i,ThreePhase(i,cluster,cluster_size))



        visitedEdges = []

        #INSERTING AND MANAGING EDGES
        for e in edges:
            if e.src == e.dest:
                continue

            visitEdges.add(e) #Show that the edge has been visited

            #FIRST HANDLE THE VERTICES AND SEE IF THEY WERE INSERTED
            if not e.src in insertedV:
                cluster[e.src%cluster_size].insertV(e.src)
                insertedV.add(e.src)

            if not e.dest in insertedV:
                cluster[e.dest%cluster_size].insertV(e.src)
                insertedV.add(e.src)

            rtn = 0

            #RTN is the return value when inserting an edge to gauge whether it has been inserted or not

            
            if e.src in splitV:
                rtn = cluster.get(e.dest % cluster_size).insertE(e.src,e.dest)

            elif e.src in locations:
                rtn = locations.get(e.src).insertE(e.src,e.dest)

            else:
                rtn = cluster.get(e.src % cluster_size).insertE(e.src,e.dest)


            if rtn < 0:
                insert_node = -1 -rtn
                cluster[insert_node].insertE(e.src,e.dest)
                locations[e.src] = insert_node
                total_reassign +=1

            elif rtn == 1:
                #THE NODE HAS BEEN SPLIT
                splitV.append(e.src)
                cluster[e.dest%cluster_size].insertE(e.src,e.dest)

            

            #DOUBT

            if len(visitedEdges) %100000 == 1 and not len(visitedEdges) == 1:
                total_cut = 0

                for edge in visited_Edges:
                    src = edge.src
                    dest = edge.dest

                    if not cluster.get(src%cluster_size).loc(src) == cluster.get(dest%cluster_size).loc(dest):
                        total_cut +=1


                print "CUTS " + str(total_cut) + " Percent: " + str(float(total_cut/len(visitedEdges)))
                

            total_cut = 0

            for edge in visited_Edges:
                src = edge.src
                dest = edge.dest

                if not cluster.get(src%cluster_size).loc(src) == cluster.get(dest%cluster_size).loc(dest):
                    total_cut +=1


            another_total_reassign = 0

            for t in cluster:
                another_total_reassign += t.reassigntimes


            memory = []

            for i in range(32):
                memory[i] =0


            for vertex in insertedV:
                src_temp = vertex%cluster_size
                actual_src = cluster[src_temp].loc.get(vertex)
                memory[actual_src] += 2
                    
                for neighbor in cluster[actual_src].v.get(src):
                    temp_dest = neighbor.dest
                    hash_dest = temp_dest%cluster_size
                    actual_dest = cluster[hash_dest].loc.get(temp_dest)

                    memory[actual_dest] +=2


            maxmem =0

            for mem in range(32):
                if memory[mem] > maxmem:
                    maxmem = memory

            print "TOTAL CUTS: " + total_cut + " Reassign: " + total_reassign + "Another reassign: " + another_total_reassign + "Percent " + float(total_cut/len(edges)) + " Memory " + maxmem
        


                        









            



        
        

    
