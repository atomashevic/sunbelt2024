import glob
import json
import sys

reddit = sys.argv[1]
folder = sys.argv[2] #'/home/anav/socio/2024_Sunbelt/data/reddit/' #pass this as parameter

files = glob.glob(folder+'/%s/core_nodes/*'%reddit)

users_dict = {}
for f in files:
    
    netid = f.split('_')[-1]
    with open(f, 'r') as F:
        l = F.readline()
        _, users = l.split('\t') 
        users = users.split()
        if len(users)>0:
            for user in users:
                curr = users_dict.get(user, [])
                curr.append(netid)
                users_dict[user] = curr
                
import json
with open(folder+'%s/%s_core_nodes.json'%(reddit, reddit), 'w') as f:
    json.dump(users_dict, f)