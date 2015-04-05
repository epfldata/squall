#!/bin/bash

# Example: ./create_tpch_db.sh tpchdb/Z1/0_01G 0.01 1
ssh dl 'cd fromhome/avitorovic/storm/bin/; ./create_and_scatter_tpch.sh '$1' '$2' '$3
sleep 10
