#!/bin/bash
#PBS -q standard
#PBS -l nodes=1:ppn=16
#PBS -e ${PBS_JOBID}.err
#PBS -o ${PBS_JOBID}.out
#PBS -l walltime=6:6:00:00

module load python/3.6.5
source $HOME/.venv/bin/activate

cd ${PBS_O_WORKDIR}

# first step is to create network
#nid_min=17
#nid_max=30
#reddit="celebrities"

NET_folder="/home/anav/socio/2024_Sunbelt/data/reddit/${reddit}/subnetworks/"
CP_folder="/home/anav/socio/2024_Sunbelt/data/reddit/${reddit}/core_nodes/"
mkdir -p ${CP_folder}


python core_periphery_subnetwork.py $nid_min $nid_max $reddit ${NET_folder} ${CP_folder}

