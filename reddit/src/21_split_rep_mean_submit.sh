#!/bin/bash
#PBS -q standard
#PBS -l nodes=1:ppn=16
#PBS -e ${PBS_JOBID}.err
#PBS -o ${PBS_JOBID}.out
#PBS -l walltime=6:6:00:00

cd ${PBS_O_WORKDIR}

module load python/3.6.5
source $HOME/.venv/bin/activate
export PATH=$HOME/zstd/zstd-dev/programs:$PATH


echo ${reputation_inline}
file=$(basename "$reputation_inline")

reddit="$(cut -d'_' -f1 <<<$file)"
pid="$(cut -d'_' -f4 <<<$file)"

folder="$HOME/socio/2024_Sunbelt/data/reddit/$reddit/${reddit}_dr_mean_/"
mkdir -p $folder
reputation_mean="$folder/${reddit}_dr_mean_${pid}"

python dynamical_reputation_v2/split_mean_dynamical_reputation.py -r local "${reputation_inline}" > "${reputation_mean}"  #--local-tmp-dir $HOME/tmp #--num-cores 40


