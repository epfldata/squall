#!/bin/bash

cp -r ../src-public-git/* ../../squall/src
rm -rf `find ../../squall/src -type d -name .svn`
