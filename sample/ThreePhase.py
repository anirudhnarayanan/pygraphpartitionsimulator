#!/usr/bin/env python

from Edge import Edge
import pdb
import sys

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

    def hash_v(self,vertex_id):
        #print "server number"
        #print self.serverNumber
        return vertex_id%self.serverNumber   #number of nodes


    def insertV(self,vertex_id):
        #print "Inserted Vertex",vertex_id
        #print self.index
        self.v[vertex_id] = set()
        self.local_neighbors[vertex_id] = 0
        self.pointed_to_me[vertex_id] = set()

        self.ra[vertex_id] = 1
        self.loc[vertex_id] = self.index
        self.split[vertex_id] = False


    def insertE(self,src, dest):
        #print "insert edge"
        #pdb.set_trace()
        newEdge = Edge(src,dest)  #create the new edge first
        #print 'reaches hash src'
        hash_src = self.hash_v(src)   #Hash and find a node for the vertex now
        #print src,dest
        #print "hash src"
        #print hash_src

        hash_dest = self.hash_v(dest)
        #print "hash_dest"
        #print hash_dest

        #print self.cluster
        dest_serv = self.cluster[hash_dest].loc[dest] #DESTINATION SERVER finding if it exists in the hash
        
        if not src in self.cluster[dest_serv].local_neighbors: #local neighbors is saying if the cluster has the node in it or not 
            self.cluster[dest_serv].local_neighbors[src] = 0 #if it doesn't then allocate it
        
        self.cluster[dest_serv].local_neighbors[src] +=1 #FIXME find a way to do an atomic increment
        
        #IMP the loc could be the same as[cluster.get , but they vary in that if the true location has/had been changed , then the record will only reflect in loc.
        if not dest in self.pointed_to_me: #to arrange for bidirectional flow
            self.pointed_to_me[dest] = set()

        self.pointed_to_me[dest].add(src)  #now dest points to source, as does source points to dest


        #vertex has been split

        if self.cluster[hash_src].split[src] == True: #DOUBTS if the hashed src has been split
            # We request the source server, and check that if the part(destination) which has been split, is in this server or not
            if not hash[dest] == self.index:    #if we are requesting the wrong server as in it has been split, and the current server is not the one which has been split and allocated
                return 1

            else:
                if not src in v:
                    v[src] = set()    #In case the split is such that there is nothing in the server itself.

                v[src].add(newEdge)   #We add a new edge. 
                return 0


        #Vertex moved but client still requests old location

        #pdb.set_trace()
        if not self.cluster[hash_src].loc[src] == self.index:   #if the hashed source location contains a different source location than the server requested
            #requesting old node
            return -1-self.cluster[hash_src].loc[src]    #over there the code does -1 - (-1 -server) = server

        else:
            self.v[src].add(newEdge)     #else add the edge
        

        #REASSIGNING VERTICES
        #print "MAX REASSIGN" + str(self.MAX_REASSIGN)
        #pdb.set_trace()
        if len(self.v.get(src)) >= (self.MAX_REASSIGN*self.cluster[hash_src].ra[src]):   #CLARIFY, checking if the lhe number of vertices is too much, and reassigning
            self.cluster[hash_src].ra[src] = self.cluster[hash_src].ra[src]*2 #CLARIFY

            from_node = self.cluster[hash_src].loc.get(src) #GET THE LOC OF THE FROM NODE
            neighbors = self.cluster[from_node].v.get(src)  #GET ITS EDGES
            max_server = from_node   #best we can do now is our server itself
            fennel_score = sys.maxint
            
            local_fennel = 0
            for part in self.cluster:
                neigh_factor = 0

                if src in part.local_neighbors: #Checks if it has any neighbors with src also whether it is in it's list of nodes
                    neigh_factor = part.local_neighbors.get(src)  #FIXME Atomic integer needed

                local_fennel = len(part.v) - neigh_factor  #number of edges needs to be given a weightage as it should be low

                #DOUBT shouldn't we calculate the fennel score of our server first, before we make a choice??
                if local_fennel < fennel_score: #we find something better
                    max_server = part.index
                    fennel_score = local_fennel

            
            #pdb.set_trace()

            if not max_server == from_node:  #DOUBT if the best possible is not our own server, then.. 
                mov_edges = self.cluster[from_node].v.get(src)  #EDGES TO MOVE
                self.cluster[from_node].v.pop(src)  #REMOVE THE SOURCE ITSELF


                self.cluster[hash_src].loc[src] = max_server  #CHANGE IT IN THE HASHED SOURCE
                self.cluster[max_server].v[src] = mov_edges   #MOVE THE EDGES ELSEWHERE

                self.reassigntimes = self.reassigntimes + 1  #We have reassigned this elsewhere


                if src in self.cluster[from_node].pointed_to_me:  
                    for vtmp in self.cluster[from_node].pointed_to_me.get(src): #for all the servers pointed to the source which is to be moved
                        if not vtmp in self.cluster[from_node].local_neighbors: #DOUBT SHOULDN'T IT BE SET TO 0 ?? if the node pointed to src isn't there on that server, then, set that one to 0
                            self.cluster[from_node].local_neighbors[vtmp] = 0 #FIXME ATOMIC
                        
                        #pdb.set_trace()
                        self.cluster[from_node].local_neighbors[vtmp] -=1 #FIXME ATOMIC DECREMENT


                if src in self.cluster[max_server].pointed_to_me:
                    for vtmp in self.cluster[max_server].pointed_to_me.get(src):
                        if not vtmp in self.cluster[max_server].local_neighbors:
                            self.cluster[max_server].local_neighbors[vtmp] = 0 #FIXME ATOMIC
                        
                        #pdb.set_trace()
                        self.cluster[max_server].local_neighbors[vtmp]+=1

                return -1-max_server #RELOCATED

            return 0

        #CHECK SPLIT

        if len(self.v.get(src)) > self.MAX_EDGES: #if it can't take anymore edges
            print "SPLITTING NOW"
            self.cluster[src].split[src] = True
            all_edges = self.v.get(src)  #get list of all my edges

            rms = []

            for edge in all_edges:  
                if not self.hash_v(edge.dest) == self.index: #if it's hashed elsewhere , then move it there
                    rms.add(edge)

            for edge in rms:
                target = self.cluster[self.hash_v(edge.dest)]
                target.insertE(edge.src,edge.dest)
                self.v.get(src).remove(edge)

            self.cluster[hash_src].loc[src] = hash_src  #DOUBT ASK WHY. IT could have relocated right?

            return 1

        return 0


    def workload_run_threshold(edges,threshold):
        self.MAX_REASSIGN = threshold
        workload_run(edges,32)

    @staticmethod
    def workload_run(edges,cluster_size):
        #pdb.set_trace()
        tota_cut = 0
        total_reassign = 0
        highest_out_degree = 0
        highest_in_degree = 0

        print len(edges)

        insertedV = []
        splitV = []
        locations = {}

        cluster = []

        #INITIALIZE CLUSTERS
        for i in range(cluster_size):
            #print "cluster init" 
            cluster.append(ThreePhase(i,cluster,cluster_size))



        visitedEdges = []

        #INSERTING AND MANAGING EDGES
        #print len(edges)
        i =0
        for e in edges:
            #print i 
            i = i+1
            if e.src == e.dest:
                continue

            visitedEdges.append(e) #Show that the edge has been visited

            #FIRST HANDLE THE VERTICES AND SEE IF THEY WERE INSERTED
            if not e.src in insertedV:
                cluster[e.src%cluster_size].insertV(e.src)
                insertedV.append(e.src)

            if not e.dest in insertedV:
                cluster[e.dest%cluster_size].insertV(e.dest)
                insertedV.append(e.dest)

            rtn = 0

            #RTN is the return value when inserting an edge to gauge whether it has been inserted or not

            
            #print "vertices done"
            if e.src in splitV:
                #print "vertex is split"
                rtn = cluster[e.dest % cluster_size].insertE(e.src,e.dest)

            elif e.src in locations: #if a location is available for it
                #print "We know another location of this vertex"
                rtn = cluster[locations.get(e.src)].insertE(e.src,e.dest)

            else:
                #print "hashing and inserting"
                #print cluster
                rtn = cluster[e.src % cluster_size].insertE(e.src,e.dest)


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

                    if not cluster[src%cluster_size].loc(src) == cluster[dest%cluster_size].loc(dest):
                        total_cut +=1


                print "CUTS " + str(total_cut) + " Percent: " + str(float(total_cut/len(visitedEdges)))
                

        total_cut = 0

        for edge in edges:
            src = edge.src
            dest = edge.dest

            if not cluster[src%cluster_size].loc[src] == cluster[dest%cluster_size].loc[dest]:
                total_cut +=1


        another_total_reassign = 0

        for t in cluster:
            another_total_reassign += t.reassigntimes


        memory = []

        for i in range(32):
            memory.append(0)


        for vertex in insertedV:
            src_temp = vertex%cluster_size
            actual_src = cluster[src_temp].loc.get(vertex)
            memory[actual_src] += 2
            #print actual_src
            #print cluster[actual_src].v
                
            for neighbor in cluster[actual_src].v.get(src,[]):
                temp_dest = neighbor.dest
                hash_dest = temp_dest%cluster_size
                actual_dest = cluster[hash_dest].loc.get(temp_dest)

                memory[actual_dest] +=2


        maxmem =0

        print memory

        for mem in range(32):
            if memory[mem] > maxmem:
                maxmem = memory[mem]

        print len(edges)
        print "TOTAL CUTS: " + str(total_cut) + " Reassign: " + str(total_reassign) + " Another reassign: " + str(another_total_reassign) + " Percent " + str(float(total_cut/float(len(edges)))) + " Memory " + str(maxmem)
    


                    









        



    
    


