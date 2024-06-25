#!/bin/bash

while read line
do
   reddit=${line} 
   
    qsub -v reddit=${reddit} transform_core_nodes_submit.sh
   

done < Subreddits