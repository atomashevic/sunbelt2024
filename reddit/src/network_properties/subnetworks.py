import pandas as pd
import networkx as nx
import json
import datetime
import time
import sys

from multiprocessing import Pool, cpu_count
import pandas as pd
import os
import gc

# load network celebrities

def read_network_line(line):
    par, comm = line.split("\t")
    par = json.loads(par.strip())
    comm = json.loads(comm.strip())

    #time = pd.to_datetime(comm['timestamp']).date  #datetime.datetime.fromtimestamp(int(comm['utc'])) #comm["utc"] #pd.to_datetime(int(comm["utc"]), unit="s")
    time = datetime.datetime.strptime(comm['timestamp'], '%Y-%m-%d').date()

    return (time, par["parent_user"], comm["author"]) #int(comm["utc"])

def get_subnetwork(network, start, L):
    
    edge_slice = []
    for i in range(netid, netid+30):
        if i in network:
           edge_slice.extend(network[i])
    return edge_slice

def write_subnetwork(filename, lista):
    with open(filename, 'w') as F:
         for line in lista:
             F.write(str(line[0]))
             F.write(";")
             F.write(str(line[1]))
             F.write(";")
             F.write(str(line[2]))
             F.write("\n")

def read_statistics_file(filename):
    
    with open(filename, "r") as F:
        file = json.load(F)
        Tmin = file["Tmin_d"]
        Tmax = file["Tmax_d"]
                
    return Tmin, Tmax


stat_file = sys.argv[1]
network_file = sys.argv[2]
results_file = sys.argv[3]


network = [] #{day: (u1, u2)}
with open(network_file, 'r') as F:
    for line in F:
        l = read_network_line(line)
        network.append(l)

Tmin, Tmax = read_statistics_file(stat_file)
Tmin = datetime.datetime.strptime(Tmin, '%Y-%m-%d').date()
Tmax = datetime.datetime.strptime(Tmax, '%Y-%m-%d').date()

day_network = {}
for line in network:
    day = (line[0]-Tmin).days    
    curr = day_network.get(day, [])
    curr.append((line[1], line[2], line[0]))
    day_network[day] = curr

del network
gc.collect()

Nsubnetw = (Tmax-Tmin).days - 30

for netid in range(Nsubnetw):
    edge_slice = []
    for i in range(netid, netid+30):
        if i in day_network:
           edge_slice.extend(day_network[i])
    write_subnetwork(results_file+"_%s"%netid, edge_slice)




