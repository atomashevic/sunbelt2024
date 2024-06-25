#!/bin/bash

while read line
do
   reddit=${line} 
   
    qsub -v reddit=${reddit} 04_transform_core_nodes_submit.sh
   

done < Subreddits4
