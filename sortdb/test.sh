#!/bin/sh
./sortdb test.tab &
sleep 2
curl -vi 'localhost:8080/get/?key=i'
curl -vi 'localhost:8080/get/?key=j'
killall sortdb
