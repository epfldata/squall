#!/bin/bash

FILENAME=~/.storm/storm.yaml
FIND=-priv
REPLACE=

echo "$FILENAME" | xargs perl -pi -e "s/$FIND/$REPLACE/g"