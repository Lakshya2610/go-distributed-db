#!/bin/bash
CURRENTDIR="$(readlink -f $0)"
CURRENTDIR=${CURRENTDIR%"/deplynode.sh"}
nohup ssh -o StrictHostKeyChecking=no $1 "cd $CURRENTDIR && cd ../DBNode && ./DBNode -id $2 -port $3 -controller $4" &
