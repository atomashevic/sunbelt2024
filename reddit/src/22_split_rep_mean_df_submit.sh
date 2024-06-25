#!/bin/bash
#PBS -q standard
#PBS -l nodes=1:ppn=16
#PBS -e ${PBS_JOBID}.err
#PBS -o ${PBS_JOBID}.out
#PBS -l walltime=6:6:00:00

cd ${PBS_O_WORKDIR}

module load python/3.6.5
source $HOME/.venv/bin/activate


FOLDER=$HOME/socio/2024_Sunbelt/data/reddit/${reddit}/${reddit}_dr_mean_/
out_file=$HOME/socio/2024_Sunbelt/data/reddit/${reddit}/${reddit}_dr_mean.csv

python dynamical_reputation_v2/convert_split_dr_to_csv.py $FOLDER $out_file

