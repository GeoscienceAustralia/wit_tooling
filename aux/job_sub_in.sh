#!/bin/bash
NCPUS=48
MEM=$((48*4))

# change this accordingly
PDYAML=fc_pd.yaml

# $1: folder with polygon list and pickled datasets
# $2: shape file

echo start to process $1 $2

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

for file in $1/query/*.pkl; do
    tile_id=$(echo $file | sed 's/[^_0-9]*//g')
    feature=$1/new/contain_$tile_id.txt
    aggregate=False
    if [ ! -s $feature ]; then
        feature=$1/new/intersect_$tile_id.txt
        if [ ! -s $feature ]; then
            echo feature list for $tile_id not exist
            continue
        else
            aggregate=True
        fi
    else
        aggregate=False
    fi
    num_thread=$(cat $feature | wc -l)
    if [ $num_thread -lt 9 ]; then
        num_thread=9
    else
        if [ $num_thread -gt $NCPUS ]; then
            num_thread=$NCPUS
        fi
    fi 

    # note: some big polygons might need dial up a bit
    mem=$((num_thread * 4))
    if [ $mem -gt $MEM ]; then
        mem=$MEM
    fi
    echo qsub -N wet_$tile_id -l ncpus=$num_thread,mem=${mem}GB -v feature=$feature,datasets=$file,aggregate=$aggregate,pdyaml=$PDYAML,shapefile=$shapefile job_normal.sh
    jobid=$(qselect -N wet_$tile_id)
    if [ "$jobid" == "" ]; then
        qsub -N wet_$tile_id -l ncpus=$num_thread,mem=${mem}GB -v feature=$feature,datasets=$file,aggregate=$aggregate,pdyaml=$PDYAML,shapefile=$shapefile job_normal.sh
    else
        qsub -W depend=afterany:$jobid -N wet_$tile_id -l ncpus=$num_thread,mem=${mem}GB -v feature=$feature,datasets=$file,aggregate=$aggregate,pdyaml=$PDYAML,shapefile=$shapefile job_normal.sh
    fi
done
