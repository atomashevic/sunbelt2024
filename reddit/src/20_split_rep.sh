#!/bin/bash
#PBS -q standard
#PBS -l nodes=1:ppn=16
#PBS -e ${PBS_JOBID}.err
#PBS -o ${PBS_JOBID}.out
#PBS -l walltime=6:6:00:00

module load python/3.6.5
source $HOME/.venv/bin/activate

cd ${PBS_O_WORKDIR}

folder="$HOME/socio/2024_Sunbelt/data/reddit/${reddit}/"

file="$folder/${reddit}_dr_inline"

split -l 100000 ${file} "${file}_" --verbose

