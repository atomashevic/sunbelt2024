import pandas as pd
import networkx as nx
import gc
import sys
import json
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

def calculate_network_properties(edge_slice):
    """
    input: edge slice - dataframe with column CommentUserId, ParentUserId
    output: dictionary with properties
    """
    network = nx.Graph()
    network = nx.from_edgelist(edge_slice)#from_pandas_edgelist(edge_slice, source="CommentUserId", target="ParentUserId")
    network = network.to_undirected()
    if len(network.nodes())!=0:
       clustering  = nx.average_clustering(network)
       N = len(network.nodes())
       L = len(network.edges())
       return {"clustering":clustering, "Nnodes":N, "L/N":L/N}
    else:
       return {"clustering":0, "Nnodes":0, "L/N":0}

class CalculateProperties(MRJob):

    def mapper(self, _, line):
        par, comm = line.split("\t")
        par = json.loads(par.strip())
        comm = json.loads(comm.strip())
        
        time = comm["utc"] #pd.to_datetime(int(comm["utc"]), unit="s")

        yield (None,  [time, par["parent_user"], comm["author"]])

    def reducer(self, _, values):
        ls = list(values)
        ls = sorted(ls, key= lambda x: int(x[0]))
        tmin = min([int(x[0]) for x in ls])
        t0 = datetime.datetime.fromtimestamp(int(tmin))
        for x in ls:
            t1 = datetime.datetime.fromtimestamp(int(x[0]))
            diff = (t1-t0).days
            yield ([ diff, x[0], tmin], [(x[1], x[2])])
    
    def mapper_subnetwork(self, time, interaction):
        ls = list(interaction)
        
        day, ts, T = time[0], time[1], time[2]
        timewindow=30
        
        nets = [day-i for i in range(int(timewindow))]
        nets =  [i for i in nets if i>=0]
        
        for n in nets:
           for pair in ls:
              yield ([n,T], pair)

    def calculate_properties(self, key, data):
       network_id, T = key
        
       edges = list(data) 
       
       results = calculate_network_properties(edges)
       results["Firstday"] = T
       yield (network_id, results)
        


    def steps(self):
        return [MRStep(mapper=self.mapper,
                        reducer=self.reducer),
                MRStep(mapper = self.mapper_subnetwork,
                       reducer = self.calculate_properties),
               ]
                     


if __name__ == '__main__':
      
     CalculateProperties.run()

        
        
        

        

