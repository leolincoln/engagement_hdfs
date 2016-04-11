#!/bin/bash
#this is a one time renaming function. 
COUNTER=0
while [  $COUNTER -lt 22  ]; do
    echo The counter is $COUNTER
    mv "cluster_centers/cluster_centers_subject"$COUNTER".csv" cluster_centers/cluster_centers_subject"$COUNTER"_.csv
    mv "cluster_sizes/cluster_sizes_subject"$COUNTER".csv" cluster_sizes/cluster_sizes_subject"$COUNTER"_.csv 
    mv max_point_distance/"$COUNTER".csv max_point_distance/"$COUNTER"_.csv
    wait
    let COUNTER=COUNTER+1
done
