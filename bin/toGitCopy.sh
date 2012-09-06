#!/bin/bash

WORK_SQUALL=/home/vitorovi/working/installations/storm/dip2/squall/src/
GIT_SQUALL=/media/sda7/github/squall/src

rm -rf $GIT_SQUALL/*
cp -r $WORK_SQUALL/* $GIT_SQUALL