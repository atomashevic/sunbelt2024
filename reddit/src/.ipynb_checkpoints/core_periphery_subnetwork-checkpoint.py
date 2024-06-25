import pandas as pd
from core_periphery_sbm import core_periphery as cp
from core_periphery_sbm import network_helper as nh
from core_periphery_sbm import model_fit as mf
import os
import networkx as nx
import sys
import json
from multiprocessing import Pool, cpu_count

def run_sbm(G):

    hubspoke = cp.HubSpokeCorePeriphery(n_gibbs=100, n_mcmc=10*len(G))
    hubspoke.infer(G)
    labels = hubspoke.get_labels(last_n_samples=50, prob=False, return_dict=True)

    #model selection
    inf_labels_hs = hubspoke.get_labels(last_n_samples=50, prob=False, return_dict=False)
    mdl_hubspoke = mf.mdl_hubspoke(G, inf_labels_hs, n_samples=100000)

    return labels, mdl_hubspoke

def mpi_run(edges):
    G = nx.Graph()
    G.add_edges_from(edges)  
    Nnodes = G.number_of_nodes()
    if G.number_of_nodes()>0:
        if G.number_of_nodes() > 500:
           f = nx.Graph()
           fedges = filter(lambda x: G.degree()[x[0]] > 1 and G.degree()[x[1]] > 1, G.edges())
           f.add_edges_from(fedges)
           labels, mdl = run_sbm(f)
        else:
           labels, mdl = run_sbm(G)
        #ns, ms, Ms = nh.get_block_stats(G, labels, n_blocks=2)
        core_nodes = [x for x, y in labels.items() if y==0.]
        if len(core_nodes)==0:
           return []
        else:
           return core_nodes
    else:
        return []

def read_network(filename):

    with open(filename, 'r') as F:
         edges = []
         times = []
         for line in F:
             u1, u2, t = line.split(";")
             edges.append((u1, u2))
             t = pd.to_datetime(t.strip(), format='%Y-%m-%d').date()

             times.append(t)
    return edges, times

def run_single_network():
    filename = sys.argv[1]
    labels_file = sys.argv[2]

    netid=filename.split('_')[-1]
    name=filename.split('_')[-3]
    edges, times = read_network(filename)
    #labels, ns, ms, Ms, mdl = mpi_run(edges)

    core_nodes = mpi_run(edges)
    Tmin = min(times)
    cn = ' '.join([str(x) for x in core_nodes])

    with open(labels_file, 'w') as F:
    #d = json.dumps(labels, default=str, indent=None)
        F.write('\t'.join([str(Tmin), cn]))

if __name__ == "__main__":

    net1 = int(sys.argv[1])#0
    net2 = int(sys.argv[2]) #15
    reddit = sys.argv[3] #'celebrities'
    NET_folder = sys.argv[4] # '/home/anav/socio/2024_Sunbelt/data/reddit/%s/subnetworks/'
    CP_folder = sys.argv[5] #'/home/anav/socio/2024_Sunbelt/data/reddit/%s/core_nodes/'
    
    networks = []
    times = []
    for i in range(net1, net2):
       filename='%s/%s_sn_%s'%(NET_folder,reddit, i)
       if os.path.exists(filename):
          edges, time = read_network(filename)
          networks.append(edges)
          times.append(time)
    
    nproc = cpu_count()
    pool = Pool(nproc)
    if len(networks)>0:
       results = pool.map(mpi_run, networks)
       
       for i in range(len(results)):
           nid = net1+i
           Tmin = min(times[i])
           cn = ' '.join([str(x) for x in results[i]])
           with open('%s/%s_cp_%s'%(CP_folder,reddit, nid), 'w') as F:
               F.write('\t'.join([str(Tmin), cn]))
               F.write('\n')
        

#load network
#run model
#write core-periphery results

