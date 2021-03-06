#!/bin/bash
#PBS -P u46
#PBS -q normal
#PBS -l storage=gdata/rs0+gdata/fk4+gdata/v10+gdata/r78+gdata/u46+scratch/r78
#PBS -l walltime=2:00:00
#PBS -l jobfs=1GB
#PBS -l wd

source $HOME/setup-datacube-up2date.sh

echo $threads $feature $datasets $aggregate $pdyaml $shapefile
export OMP_NUM_THREADS=$threads
export NUMEXPR_MAX_THREADS=$threads
mpirun -np 9 -bind-to none python3 -m mpi4py.futures wetland_brutal.py wit-cal --feature-list $feature --datasets $datasets --aggregate $aggregate --product-yaml $pdyaml $shapefile
