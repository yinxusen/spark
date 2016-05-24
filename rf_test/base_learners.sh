#!/bin/bash
../bin/run-example ml.AirlineRFC ~/data/airline/train-0.01m.csv ~/data/airline/test.csv --numTrees 1 --maxDepth 20
../bin/run-example ml.AirlineRFC ~/data/airline/train-0.1m.csv ~/data/airline/test.csv --numTrees 1 --maxDepth 20
../bin/run-example ml.AirlineRFC ~/data/airline/train-1m.csv ~/data/airline/test.csv --numTrees 1 --maxDepth 20
../bin/run-example ml.AirlineRFC ~/data/airline/train-10m.csv ~/data/airline/test.csv --numTrees 1 --maxDepth 20
