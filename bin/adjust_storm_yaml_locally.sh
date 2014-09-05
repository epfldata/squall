#!/bin/bash

FILENAME=~/.storm/storm.yaml
FIND=master3
REPLACE=icdataportal3

echo "$FILENAME" | xargs perl -pi -e "s/$FIND/$REPLACE/g"
