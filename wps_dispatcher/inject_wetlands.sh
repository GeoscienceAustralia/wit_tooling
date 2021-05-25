#!/bin/bash
export SHAPE_URL=https://data.dea.ga.gov.au/projects/Wetlands_Insight_Tool/QLD/Queensland_dominant_wetland_areas_22042020.zip
export QLD_DB=qld_wetlands
export PGPASSWORD=mysecretpassword
ogrinfo -ro -al -so /vsizip/vsicurl/$SHAPE_URL
wget $SHAPE_URL
unzip Queensland_dominant_wetland_areas_22042020.zip
docker-compose up -d
psql -h localhost -p 5432 -U postgres -d postgres -c "create database $QLD_DB"
psql -h localhost -p 5432 -U postgres -d qld_wetlands -c "create extension postgis"
shp2pgsql -s 3577 -d Queensland_dominant_wetland_areas_22042020.shp $QLD_DB | psql -h localhost -p 5432 -U postgres -d qld_wetlands
# TODO: Figure out how to read the shapefile DB from OGR
# ogrinfo -ro PG:"host='localhost' user='postgres' dbname='$QLD_DB'"