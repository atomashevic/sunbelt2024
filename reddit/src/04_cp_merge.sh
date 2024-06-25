#!/bin/bash

while read line
do  
   reddit=${line}  
   cat ../data/reddit/${reddit}/core_nodes/* > ../data/reddit/${reddit}/${reddit}_core_nodes

done < Subreddits4
