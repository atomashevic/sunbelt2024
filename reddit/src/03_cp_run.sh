#!/bin/bash

while read line
do
   reddit=${line} 
   for i in {0..94}
   do
     nid_min=$(( 32*$i))
     nid_max=$(( 32*$i + 32 ))
     echo $nid_min $nid_max
     qsub -v reddit=${reddit},nid_min=$nid_min,nid_max=$nid_max  03_cp_submit.sh
   done

done < Subreddits4



