#!/bin/bash

for reddit in gaming pics funny
do
   for reputation_inline in $HOME/socio/2024_Sunbelt/data/reddit/${reddit}/${reddit}_dr_inline_/**
   do
      echo $reputation_inline
       qsub -v reputation_inline=${reputation_inline}  21_split_rep_mean_submit.sh
   done
done



