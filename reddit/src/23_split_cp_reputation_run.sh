#!/bin/bash

for reddit in gaming  #gifs pics funny #  videos  #MusicVideos gaming gifs pics technology videos video videogames
do
for reputation_inline in $HOME/socio/2024_Sunbelt/data/reddit/${reddit}/${reddit}_dr_inline_/**
   do
      echo $reputation_inline
 
      qsub -v reputation_inline=${reputation_inline} 23_split_cp_reputation_submit.sh
   done

done



