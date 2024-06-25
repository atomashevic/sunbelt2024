import sys
import json

def read_file(fname):
    data={}
    with open(fname, "r") as fn:
        for line in fn:
            l = json.loads(line)
            user=l["author"]
            time=int(l["created_utc"])
            curr = data.get(user, [])
            curr.append(time)
            data[user]=curr
    return data

def write_file(fname, users_ts):

   with open(fname, "w") as fn:
       for key, value in users_ts.items():
           fn.write(str(key))
           fn.write("\t")
           fn.write(json.dumps(value))
           fn.write("\n")

sfile = sys.argv[1]
cfile = sys.argv[2]
outfile = sys.argv[3]

submissions = read_file(sfile)
comments = read_file(cfile)

users_ts = {}

for key, value in submissions.items():
    
    users_ts[key] = {"S":value}

for key, value in comments.items():
    
    if key in users_ts:
       users_ts[key]["C"] = value
    
    else:
       users_ts[key] = {"C":value}

write_file(outfile, users_ts)
