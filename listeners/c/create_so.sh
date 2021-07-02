#!/bin/bash




# Alpine
echo "Alpine: Building Image"
docker build . -q -f base-images/alpine/Dockerfile -t alpine-gcc
echo "Alpine: Running Make in container"
docker run --log-driver none --rm -v $(pwd):/foo -w /foo alpine-gcc make -s
echo "Alpine: Moving module.so to bin/module_alpine.so"
mv module.so bin/module_alpine.so
echo "Alpine: Done"

## Debian
#echo "Debian: Building Image"
#docker build . -q -f base-images/debian/Dockerfile -t debian-gcc
#echo "Debian: Running Make in container"
#docker run --log-driver none --rm -v $(pwd):/foo -w /foo debian-gcc make -s
#echo "Debian: Moving module.so to bin/module_debian.so"
#mv module.so bin/module_debian.so
#echo "Debian: Done"
#
## Ubuntu
#echo "Ubuntu: Building Image"
#docker build . -q -f base-images/ubuntu/Dockerfile -t ubuntu-gcc
#echo "Ubuntu: Running Make in container"
#docker run --log-driver none --rm -v $(pwd):/foo -w /foo ubuntu-gcc make -s
#echo "Ubuntu: Moving module.so to bin/module_ubuntu.so"
#mv module.so bin/module_ubuntu.so
#echo "Ubuntu: Done"

## OSX - Assumes script is running on OSX
#echo "OSX: Running Make"
#make -s all
#echo "OSX: Moving module.so to bin/module_osx.so"
#mv module.so bin/module_osx.so
#make -s clean
#echo "OSX: Done"
