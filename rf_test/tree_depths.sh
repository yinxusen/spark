#!/bin/bash
#../bin/run-example ml.AirlineRFC ~/data/airline/train-10m.csv ~/data/airline/test.csv --numTrees 1 --maxDepth 5 
#../bin/run-example ml.AirlineRFC ~/data/airline/train-10m.csv ~/data/airline/test.csv --numTrees 1 --maxDepth 10
../bin/run-example ml.AirlineRFC ~/data/airline/train-10m.csv ~/data/airline/test.csv --numTrees 1 --maxDepth 20 --maxMemoryInMB 500 --numPartitions 1
