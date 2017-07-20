#!/bin/bash
echo "start converting"
kml=$1
shape=$2'shapefile.shp'
dir=$2
d=$2'output*'
ogr2ogr -f 'ESRI Shapefile' $shape $kml
#zip the result
#cd $2
#tar -zcvf EsriShapeFile.tar.gz ./output.*
#remove generated shape files
#rm -rf $d

