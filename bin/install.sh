#!/bin/bash

./lein plugin install lein-localrepo "0.3"
./lein localrepo install ../contrib/trove-3.0.2.jar trove 3.0.2

mkdir -p /tmp/ramdisk
