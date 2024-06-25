#!/bin/bash

for reddit in gaming pics videos funny
do
   # reddit=${line} 
   
    qsub -v reddit=${reddit} 20_split_rep.sh
   

done
