#!/bin/bash -ve

HERE=`dirname "$0"`
cd $HERE/..

for i in 39; do
    docker build -f docker/py${i}-wheel-manylinux2014.docker . -t casa-arrow-py${i}-wheel
    dockerid=$(docker create casa-arrow-py${i}-wheel)
    docker cp ${dockerid}:/wheels/ wheel-${i}
    docker rm ${dockerid}
done
