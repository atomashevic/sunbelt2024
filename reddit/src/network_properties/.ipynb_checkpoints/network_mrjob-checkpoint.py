
from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import pandas as pd

class CreateNetwork(MRJob):

    def mapper(self, _, line):
        data = json.loads(line)
        
        result = {"id": data["id"], 
                  "author": data["author"],
                  "created_utc": data["created_utc"],
                  "timestamp": data["timestamp"]}

       
        if "parent_id" in data:
            result["parent_id"] = data["parent_id"].split("_")[1]
            result["post"]="comment"
        else:
            result["parent_id"] = (data["id"])
            result["post"]="submission"
        yield(result["id"], result)
        yield(result["parent_id"], result)

    def reducer(self, key, values):
        ls = list(values)
        parent_id=None
        for val in ls:
            if key==val["id"]:
                parent_id = key
                parent_time = val["created_utc"]
                parent_user = val["author"]
                parent_post = val["post"]
                parent_timestamp = val["timestamp"]
        if (parent_id!=None) and (parent_post=="submission"):
            for val in ls:
                if key!=val["id"]:           
                   yield({"parent_id": parent_id, "parent_user": parent_user, "parent_utc":parent_time, "parent_timestamp":parent_timestamp,  "parent_post":parent_post}, 
                          {"id": val["id"], "author": val["author"], "utc": val["created_utc"], "timestamp":val["timestamp"], "post": val["post"]} )
                 

    def steps(self):
        return [MRStep(mapper=self.mapper,
                       reducer=self.reducer,
                )]

if __name__ == '__main__':
    CreateNetwork.run()
