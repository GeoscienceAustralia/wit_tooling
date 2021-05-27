#!/bin/bash
NCPUS=48
MEM=$((48*4))
#how much memory charged over 1 cpu
UMEM=4

# change this accordingly
PDYAML=fc_pd.yaml

# $1: folder with polygon list and pickled datasets
# $2: shape file
# $3: days interval if aggregate

echo start to process $1 $2 $3

if [ ! -s $2 ]; then
    echo shape file $2 not exist
    exit
fi

if [ ! -d $1/query ]; then
    echo query results should be in $1/query
    exit
fi

if [ ! -d $1/new ]; then
    echo feature lists should be in $1/new
    exit
fi

PDYAML=$(readlink -f $PDYAML)
shapefile=$(readlink -f $2)
AGGREGATE=$3

for file in $1/query/*.pkl; do
    tile_id=$(echo $file | sed 's/.*\/\([_0-9]\+\).*/\1/g')
    feature=$1/new/contain_$tile_id.txt
    aggregate=0
    if [ ! -s $feature ]; then
        feature=$1/new/intersect_$tile_id.txt
        if [ ! -s $feature ]; then
            echo feature list for $tile_id not exist
            continue
        else
            # note: some big polygons might need dial up a bit
            # we dial up to double for aggregation over time slices
            aggregate=$AGGREGATE
            UMEM=8
        fi
    else
        aggregate=0
        UMEM=4
    fi
    num_thread=$(cat $feature | wc -l)
    if [ $num_thread -lt 9 ]; then
        num_thread=9
    else
        if [ $num_thread -gt $NCPUS ]; then
            num_thread=$NCPUS
        fi
    fi 

    mem=$((num_thread * UMEM))
    if [ $mem -gt $MEM ]; then
        mem=$MEM
    fi
    echo qsub -N ${1//\/}_$tile_id -l ncpus=$num_thread,mem=${mem}GB -v threads=$((num_thread * 4)),feature=$feature,datasets=$file,aggregate=$aggregate,pdyaml=$PDYAML,shapefile=$shapefile job_normal.sh
    jobid=$(qselect -N ${1//\/}_$tile_id)
    if [ "$jobid" == "" ]; then
        qsub -N ${1//\/}_$tile_id -l ncpus=$num_thread,mem=${mem}GB -v threads=$((num_thread * 4)),feature=$feature,datasets=$file,aggregate=$aggregate,pdyaml=$PDYAML,shapefile=$shapefile job_normal.sh
    else
        for i in $(seq 1 2); do
            jobid=$(qsub -W depend=afterany:$jobid -N ${1//\/}_$tile_id -l ncpus=$num_thread,mem=${mem}GB -v threads=$((num_thread * 4)),feature=$feature,datasets=$file,aggregate=$aggregate,pdyaml=$PDYAML,shapefile=$shapefile job_normal.sh)
        done
    fi
done
