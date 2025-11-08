#!/bin/bash

BASE_PORT=50000
if [[ $# == 0  || $# > 1 ]]
    then
        echo "You need to specify a number of nodes to create the network with"
        exit 1
    else
        N=$1 
        for i in $(seq 1 $N); do
            LOCAL_PORT=$((BASE_PORT+i))
            PEERS=""
            for j in $(seq 1 $N); do
                if [[ $i == $j ]]
                    then
                        continue 
                fi
                PEER_PORT=$((BASE_PORT+j))
                PEERS+="localhost:$PEER_PORT,"
            done

            PEERS="${PEERS%,}"
            go run main.go $i $LOCAL_PORT $PEERS &
        done
fi