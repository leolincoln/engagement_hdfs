#!/bin/bash 
COUNTER=7
while [  $COUNTER -lt 16  ]; do
    echo The counter is $COUNTER
    python corr_matrix_subject.py $COUNTER cluster_centers/ max_point_distance/
    wait
    let COUNTER=COUNTER+1 
done
