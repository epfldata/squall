#!/bin/bash

./lein plugin install lein-localrepo "0.3"
./lein localrepo install ../contrib/trove-3.0.2.jar trove 3.0.2
./lein localrepo install ../contrib/bheaven-0.0.3.jar bheaven 0.0.3

mkdir -p /tmp/ramdisk
