#!/bin/bash

FILENAME=~/.storm/$1
FIND=-priv
REPLACE=

echo "$FILENAME" | xargs perl -pi -e "s/$FIND/$REPLACE/g"