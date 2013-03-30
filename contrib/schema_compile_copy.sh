#!/bin/bash

javacc SchemaParser.jj
cp *.java ~/working/installations/storm/dip2/squall/src/sql/schema/parser
