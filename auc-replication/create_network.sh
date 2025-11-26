#!/bin/bash

BASE_PORT=50000
if [[ $# != 1 ]]
    then
        echo "You need to specify both a number of client instances to create the network with"
        exit 1
    else
        servers=2
        clients=$1
        cd servers
        for i in $(seq 1 $servers); do
            LOCAL_PORT=$((BASE_PORT+i))
            go run main.go $LOCAL_PORT > ./server_${LOCAL_PORT}.log 2>&1 &
            sleep 1
        done
        cd ../clients
        for i in $(seq 1 $clients); do
            go run main.go $i > ./client_${i}.log 2>&1 &
            sleep 2
        done

        wait
fi